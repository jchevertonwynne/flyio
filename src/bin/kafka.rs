use anyhow::{Context, bail};
use async_trait::async_trait;
use flyio::{Body, Init, KvError, LinKvClient, Message, MsgIDProvider, Node, main_loop};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ops::Deref, sync::Arc, time::Duration};
use tokio::sync::mpsc::Sender;
use tracing::{error, warn};

#[derive(Clone)]
struct Kafka {
    inner: Arc<KafkaNodeInner>,
}

struct KafkaNodeInner {
    #[allow(dead_code)]
    node_id: String,
    #[allow(dead_code)]
    id_provider: MsgIDProvider,

    kv: LinKvClient,
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
        let Message { src, dst, body } = msg;
        let Body {
            incoming_msg_id,
            in_reply_to: _,
            payload: (),
        } = body;
        let Self { key, msg } = self;

        let kv_key = format!("kafka:{key}");
        let (mut remote, mut create): (MessageDetails, bool) =
            match node.kv.read(kv_key.clone()).await {
                Ok(data) => (data, false),
                Err(KvError::KeyDoesNotExist) => (MessageDetails::default(), true),
                Err(e) => return Err(e.into()),
            };

        let mut sleep_dur = Duration::from_millis(1);
        let offset = loop {
            let mut entry = remote.clone();

            let offset = entry.next_offset;
            entry.next_offset += 1;
            entry.messages.push(MessageEntry { offset, msg });

            match node
                .kv
                .compare_and_swap(kv_key.clone(), &remote, &entry, create)
                .await
            {
                Ok(true) => break offset,
                Ok(false) => {
                    create = false;
                    remote = match node.kv.read(kv_key.clone()).await {
                        Ok(data) => data,
                        Err(KvError::KeyDoesNotExist) => MessageDetails::default(),
                        Err(e) => return Err(e.into()),
                    };
                }
                Err(KvError::KeyDoesNotExist) => {
                    remote = MessageDetails::default();
                    create = true;
                }
                Err(e) => return Err(e.into()),
            }

            tokio::time::sleep(sleep_dur).await;
            sleep_dur = (sleep_dur * 2).min(Duration::from_millis(100));
        };

        let resp_msg_id = node.id_provider.id();
        let response = Message {
            src: dst,
            dst: src,
            body: Body {
                incoming_msg_id: Some(resp_msg_id),
                in_reply_to: incoming_msg_id,
                payload: KafkaNodePayload::SendOk(SendOk { offset }),
            },
        };

        tx.send(response).await.context("channel closed")?;

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
        let _ = self;
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
        let mut result = HashMap::new();

        for (key, offset) in offsets {
            match node.kv.read::<MessageDetails>(format!("kafka:{key}")).await {
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
                    if !filtered.is_empty() {
                        result.insert(key, filtered);
                    }
                }
                Err(KvError::KeyDoesNotExist) => {}
                Err(e) => return Err(e.into()),
            }
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
        let _ = self;
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

        for (key, offset) in offsets {
            let key = format!("kafka:{key}");
            let mut entry = match node.kv.read::<MessageDetails>(key.clone()).await {
                Ok(entry) => entry,
                Err(KvError::KeyDoesNotExist) => continue,
                Err(e) => return Err(e.into()),
            };

            let mut sleep_dur = Duration::from_millis(1);
            'cas: loop {
                let mut entry_copy = entry.clone();
                entry_copy.committed_offset = entry_copy.committed_offset.max(offset);

                match node
                    .kv
                    .compare_and_swap(key.clone(), &entry, &entry_copy, false)
                    .await
                {
                    Ok(true) | Err(KvError::KeyDoesNotExist) => break 'cas,
                    Ok(false) => {
                        entry = match node.kv.read(key.clone()).await {
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
        }

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
        let _ = self;
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

        let mut result = HashMap::new();
        for key in keys {
            match node.kv.read::<MessageDetails>(format!("kafka:{key}")).await {
                Ok(val) => {
                    result.insert(key, val.committed_offset);
                }
                Err(KvError::KeyDoesNotExist) => {
                    result.insert(key, 0);
                }
                Err(e) => return Err(e.into()),
            }
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
        let _ = self;
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

impl KafkaNodePayload {
    async fn dispatch(
        self,
        msg: Message<Body<()>>,
        node: &Kafka,
        tx: Sender<Message<Body<KafkaNodePayload>>>,
    ) -> anyhow::Result<()> {
        match self {
            KafkaNodePayload::Send(payload) => payload.handle(msg, node, tx).await,
            KafkaNodePayload::SendOk(payload) => payload.handle(msg, node, tx).await,
            KafkaNodePayload::Poll(payload) => payload.handle(msg, node, tx).await,
            KafkaNodePayload::PollOk(payload) => payload.handle(msg, node, tx).await,
            KafkaNodePayload::CommitOffsets(payload) => payload.handle(msg, node, tx).await,
            KafkaNodePayload::CommitOffsetsOk(payload) => payload.handle(msg, node, tx).await,
            KafkaNodePayload::ListCommittedOffsets(payload) => payload.handle(msg, node, tx).await,
            KafkaNodePayload::ListCommittedOffsetsOk(payload) => {
                payload.handle(msg, node, tx).await
            }
            KafkaNodePayload::Error(payload) => payload.handle(msg, node, tx).await,
        }
    }
}

#[async_trait]
impl Node<LinKvClient, ()> for Kafka {
    type Payload = KafkaNodePayload;

    async fn from_init(
        init: Init,
        lin_kv: LinKvClient,
        id_provider: MsgIDProvider,
    ) -> anyhow::Result<Self> {
        Ok(Kafka {
            inner: Arc::new(KafkaNodeInner {
                node_id: init.node_id,
                id_provider,
                kv: lin_kv,
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


}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop::<Kafka, LinKvClient, ()>()
        .await
        .inspect_err(|err| error!("failed to run main: {err}"))
}
