use anyhow::{Context, bail};
use async_trait::async_trait;
use flyio::{Body, Init, KvError, LinKvClient, Message, MsgIDProvider, Node, Worker, main_loop};
use futures::future::try_join_all;
use serde::ser::SerializeTuple;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ops::Deref, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc::Sender};
use tracing::{error, warn};

#[derive(Clone)]
struct Kafka {
    inner: Arc<KafkaNodeInner>,
}

struct KafkaNodeInner {
    id_provider: MsgIDProvider,

    kv: LinKvClient,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    pending_sends: HashMap<String, Vec<PendingSend>>,
}

struct PendingSend {
    msg: u64,
    client_msg: Message<Body<()>>,
    tx: Sender<Message<Body<KafkaNodePayload>>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct MessageDetails {
    committed_offset: u64,
    next_offset: u64,
    messages: Vec<MessageEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MessageEntry {
    offset: u64,
    msg: u64,
}

impl Deref for Kafka {
    type Target = KafkaNodeInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum KafkaNodePayload {
    Send(Send),
    SendOk(SendOk),
    Poll(Poll),
    PollOk(PollOk),
    CommitOffsets(CommitOffsets),
    CommitOffsetsOk(CommitOffsetsOk),
    ListCommittedOffsets(ListCommittedOffsets),
    ListCommittedOffsetsOk(ListCommittedOffsetsOk),
    Error(Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
struct Send {
    key: String,
    msg: u64,
}

impl Send {
    async fn handle(
        self,
        msg: Message<Body<()>>,
        node: &Kafka,
        tx: Sender<Message<Body<KafkaNodePayload>>>,
    ) -> anyhow::Result<()> {
        let Self { key, msg: val } = self;
        let pending = PendingSend {
            msg: val,
            client_msg: msg,
            tx,
        };

        let mut state = node.state.lock().await;
        state.pending_sends.entry(key).or_default().push(pending);

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct SendOk {
    offset: u64,
}

impl SendOk {
    #[allow(clippy::unused_async)]
    async fn handle(
        self,
        _msg: Message<Body<()>>,
        _node: &Kafka,
        _tx: Sender<Message<Body<KafkaNodePayload>>>,
    ) -> anyhow::Result<()> {
        bail!("unexpected SendOk message")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Poll {
    offsets: HashMap<String, u64>,
}

impl Poll {
    async fn handle(
        self,
        msg: Message<Body<()>>,
        node: &Kafka,
        tx: Sender<Message<Body<KafkaNodePayload>>>,
    ) -> anyhow::Result<()> {
        let Message { src, dst, body } = msg;
        let Body {
            incoming_msg_id,
            in_reply_to: _,
            payload: (),
        } = body;
        let Self { offsets } = self;
        let read_futs = offsets.into_iter().map(|(key, offset)| {
            let kv = node.kv.clone();
            async move {
                match kv.read::<MessageDetails>(format!("kafka:{key}")).await {
                    Ok(details) => {
                        let filtered: Vec<PollOkMessageEntry> = details
                            .messages
                            .iter()
                            .filter(|&entry| entry.offset >= offset)
                            .map(|entry| PollOkMessageEntry {
                                offset: entry.offset,
                                msg: entry.msg,
                            })
                            .collect();
                        if filtered.is_empty() {
                            Ok(None)
                        } else {
                            Ok(Some((key, filtered)))
                        }
                    }
                    Err(KvError::KeyDoesNotExist) => Ok(None),
                    Err(e) => Err(e),
                }
            }
        });

        let mut result = HashMap::new();
        for (key, filtered) in (try_join_all(read_futs).await?).into_iter().flatten() {
            result.insert(key, filtered);
        }

        let resp_msg_id = node.id_provider.id();
        let response = Message {
            src: dst,
            dst: src,
            body: Body {
                incoming_msg_id: Some(resp_msg_id),
                in_reply_to: incoming_msg_id,
                payload: KafkaNodePayload::PollOk(PollOk { msgs: result }),
            },
        };
        tx.send(response).await.context("channel closed")?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct PollOk {
    msgs: HashMap<String, Vec<PollOkMessageEntry>>,
}

#[derive(Debug, Clone)]
struct PollOkMessageEntry {
    offset: u64,
    msg: u64,
}

impl Serialize for PollOkMessageEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_tuple(2)?;
        state.serialize_element(&self.offset)?;
        state.serialize_element(&self.msg)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for PollOkMessageEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (offset, msg) = <(u64, u64)>::deserialize(deserializer)?;
        Ok(PollOkMessageEntry { offset, msg })
    }
}

impl PollOk {
    #[allow(clippy::unused_async)]
    async fn handle(
        self,
        _msg: Message<Body<()>>,
        _node: &Kafka,
        _tx: Sender<Message<Body<KafkaNodePayload>>>,
    ) -> anyhow::Result<()> {
        bail!("unexpected PollOk message")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct CommitOffsets {
    offsets: HashMap<String, u64>,
}

impl CommitOffsets {
    async fn handle(
        self,
        msg: Message<Body<()>>,
        node: &Kafka,
        tx: Sender<Message<Body<KafkaNodePayload>>>,
    ) -> anyhow::Result<()> {
        let Message { src, dst, body } = msg;
        let Body {
            incoming_msg_id,
            in_reply_to: _,
            payload: (),
        } = body;
        let Self { offsets } = self;
        let commit_tasks = offsets.into_iter().map(|(key, offset)| {
            let node = node.clone();
            async move { node.commit_offset_for_key(key, offset).await }
        });

        try_join_all(commit_tasks).await?;

        let resp_msg_id = node.id_provider.id();
        let response = Message {
            src: dst,
            dst: src,
            body: Body {
                incoming_msg_id: Some(resp_msg_id),
                in_reply_to: incoming_msg_id,
                payload: KafkaNodePayload::CommitOffsetsOk(CommitOffsetsOk),
            },
        };
        tx.send(response).await.context("channel closed")?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct CommitOffsetsOk;

impl CommitOffsetsOk {
    #[allow(clippy::unused_async)]
    async fn handle(
        self,
        _msg: Message<Body<()>>,
        _node: &Kafka,
        _tx: Sender<Message<Body<KafkaNodePayload>>>,
    ) -> anyhow::Result<()> {
        bail!("unexpected CommitOffsetsOk message")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct ListCommittedOffsets {
    keys: Vec<String>,
}

impl ListCommittedOffsets {
    async fn handle(
        self,
        msg: Message<Body<()>>,
        node: &Kafka,
        tx: Sender<Message<Body<KafkaNodePayload>>>,
    ) -> anyhow::Result<()> {
        let Message { src, dst, body } = msg;
        let Body {
            incoming_msg_id,
            in_reply_to: _,
            payload: (),
        } = body;
        let Self { keys } = self;
        let kv = node.kv.clone();
        let read_futs = keys.into_iter().map(|key| {
            let kv = kv.clone();
            async move {
                match kv.read::<MessageDetails>(format!("kafka:{key}")).await {
                    Ok(val) => Ok((key, val.committed_offset)),
                    Err(KvError::KeyDoesNotExist) => Ok((key, 0)),
                    Err(e) => Err(e),
                }
            }
        });

        let mut result = HashMap::new();
        for (key, offset) in try_join_all(read_futs).await? {
            result.insert(key, offset);
        }

        let resp_msg_id = node.id_provider.id();
        let response = Message {
            src: dst,
            dst: src,
            body: Body {
                incoming_msg_id: Some(resp_msg_id),
                in_reply_to: incoming_msg_id,
                payload: KafkaNodePayload::ListCommittedOffsetsOk(ListCommittedOffsetsOk {
                    offsets: result,
                }),
            },
        };
        tx.send(response).await.context("channel closed")?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct ListCommittedOffsetsOk {
    offsets: HashMap<String, u64>,
}

impl ListCommittedOffsetsOk {
    #[allow(clippy::unused_async)]
    async fn handle(
        self,
        _msg: Message<Body<()>>,
        _node: &Kafka,
        _tx: Sender<Message<Body<KafkaNodePayload>>>,
    ) -> anyhow::Result<()> {
        bail!("unexpected ListCommittedOffsetsOk message")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Error {
    code: u64,
    text: String,
}

impl Error {
    #[allow(clippy::unused_async)]
    async fn handle(
        self,
        _msg: Message<Body<()>>,
        _node: &Kafka,
        _tx: Sender<Message<Body<KafkaNodePayload>>>,
    ) -> anyhow::Result<()> {
        let Self { code, text } = self;
        warn!("error payload recieved: code = {code} text = {text}");

        Ok(())
    }
}

impl Kafka {
    async fn process_send_batch(self, key: String, batch: Vec<PendingSend>) {
        let kv_key = format!("kafka:{key}");
        let (mut remote, mut create): (MessageDetails, bool) =
            match self.kv.read(kv_key.clone()).await {
                Ok(data) => (data, false),
                Err(KvError::KeyDoesNotExist) => (MessageDetails::default(), true),
                Err(e) => {
                    error!("failed to read kv for batch: {e}");
                    return;
                }
            };

        let mut sleep_dur = Duration::from_millis(1);
        let start_offset = loop {
            let mut entry = remote.clone();
            let mut next_offset = entry.next_offset;

            for pending in &batch {
                entry.messages.push(MessageEntry {
                    offset: next_offset,
                    msg: pending.msg,
                });
                next_offset += 1;
            }
            entry.next_offset = next_offset;

            match self
                .kv
                .compare_and_swap(kv_key.clone(), &remote, &entry, create)
                .await
            {
                Ok(true) => break entry.next_offset - batch.len() as u64,
                Ok(false) => {
                    create = false;
                    remote = match self.kv.read(kv_key.clone()).await {
                        Ok(data) => data,
                        Err(KvError::KeyDoesNotExist) => MessageDetails::default(),
                        Err(e) => {
                            error!("failed to re-read kv for batch: {e}");
                            return;
                        }
                    };
                }
                Err(KvError::KeyDoesNotExist) => {
                    remote = MessageDetails::default();
                    create = true;
                }
                Err(e) => {
                    error!("cas failed for batch: {e}");
                    return;
                }
            }

            tokio::time::sleep(sleep_dur).await;
            sleep_dur = (sleep_dur * 2).min(Duration::from_millis(100));
        };

        for (i, pending) in batch.into_iter().enumerate() {
            let offset = start_offset + i as u64;
            let Message { src, dst, body } = pending.client_msg;
            let Body {
                incoming_msg_id,
                in_reply_to: _,
                payload: (),
            } = body;

            let resp_msg_id = self.id_provider.id();
            let response = Message {
                src: dst,
                dst: src,
                body: Body {
                    incoming_msg_id: Some(resp_msg_id),
                    in_reply_to: incoming_msg_id,
                    payload: KafkaNodePayload::SendOk(SendOk { offset }),
                },
            };

            if let Err(e) = pending.tx.send(response).await {
                warn!("failed to send batch response: {e}");
            }
        }
    }

    async fn commit_offset_for_key(&self, key: String, offset: u64) -> anyhow::Result<()> {
        let key = format!("kafka:{key}");
        let mut entry = match self.kv.read::<MessageDetails>(key.clone()).await {
            Ok(entry) => entry,
            Err(KvError::KeyDoesNotExist) => return Ok(()),
            Err(e) => return Err(e.into()),
        };

        let mut sleep_dur = Duration::from_millis(1);
        'cas: loop {
            let mut entry_copy = entry.clone();
            entry_copy.committed_offset = entry_copy.committed_offset.max(offset);
            let cutoff = entry_copy
                .messages
                .partition_point(|msg| msg.offset < entry_copy.committed_offset);
            entry_copy.messages.drain(..cutoff);

            match self
                .kv
                .compare_and_swap(key.clone(), &entry, &entry_copy, false)
                .await
            {
                Ok(true) | Err(KvError::KeyDoesNotExist) => break 'cas,
                Ok(false) => {
                    entry = match self.kv.read(key.clone()).await {
                        Ok(next) => next,
                        Err(KvError::KeyDoesNotExist) => break 'cas,
                        Err(e) => return Err(e.into()),
                    };
                }
                Err(e) => return Err(e.into()),
            }

            tokio::time::sleep(sleep_dur).await;
            sleep_dur = (sleep_dur * 2).min(Duration::from_millis(100));
        }

        Ok(())
    }
}

impl KafkaNodePayload {
    async fn dispatch(
        self,
        msg: Message<Body<()>>,
        node: &Kafka,
        tx: Sender<Message<Body<KafkaNodePayload>>>,
    ) -> anyhow::Result<()> {
        use KafkaNodePayload::{
            CommitOffsets, CommitOffsetsOk, Error, ListCommittedOffsets, ListCommittedOffsetsOk,
            Poll, PollOk, Send, SendOk,
        };
        match self {
            Send(payload) => payload.handle(msg, node, tx).await,
            SendOk(payload) => payload.handle(msg, node, tx).await,
            Poll(payload) => payload.handle(msg, node, tx).await,
            PollOk(payload) => payload.handle(msg, node, tx).await,
            CommitOffsets(payload) => payload.handle(msg, node, tx).await,
            CommitOffsetsOk(payload) => payload.handle(msg, node, tx).await,
            ListCommittedOffsets(payload) => payload.handle(msg, node, tx).await,
            ListCommittedOffsetsOk(payload) => payload.handle(msg, node, tx).await,
            Error(payload) => payload.handle(msg, node, tx).await,
        }
    }
}

#[async_trait]
impl Worker<KafkaNodePayload> for Kafka {
    fn tick_interval(&self) -> Option<Duration> {
        Some(Duration::from_millis(100))
    }

    async fn handle_tick(
        &self,
        _tx: Sender<Message<Body<KafkaNodePayload>>>,
    ) -> anyhow::Result<()> {
        let mut sends = HashMap::new();
        {
            let mut state = self.inner.state.lock().await;
            std::mem::swap(&mut state.pending_sends, &mut sends);
        }

        for (key, batch) in sends {
            let node = self.clone();
            tokio::spawn(async move {
                node.process_send_batch(key, batch).await;
            });
        }

        Ok(())
    }
}

#[async_trait]
impl Node<LinKvClient, Self> for Kafka {
    type Payload = KafkaNodePayload;

    async fn from_init(
        _init: Init,
        lin_kv: LinKvClient,
        id_provider: MsgIDProvider,
    ) -> anyhow::Result<Self> {
        Ok(Kafka {
            inner: Arc::new(KafkaNodeInner {
                id_provider,
                kv: lin_kv,
                state: Mutex::new(State::default()),
            }),
        })
    }

    async fn handle(
        &self,
        msg: Message<Body<Self::Payload>>,
        tx: Sender<Message<Body<Self::Payload>>>,
    ) -> anyhow::Result<()> {
        let (payload, message) = msg.replace_payload(());
        payload.dispatch(message, self, tx).await
    }

    fn get_worker(&self) -> Option<Self> {
        Some(self.clone())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop::<Kafka, LinKvClient, Kafka>()
        .await
        .inspect_err(|err| error!("failed to run main: {err}"))
}
