use anyhow::{Context, bail};
use enum_dispatch::enum_dispatch;
use flyio::{Body, Init, Message, MsgIDProvider, Node, main_loop};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ops::Deref, sync::Arc, time::Duration};
use tokio::{
    select,
    sync::{Mutex, mpsc::Sender},
    time::MissedTickBehavior,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
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
    cancel: CancellationToken,
    tasks: TaskTracker,

    messages: Mutex<HashMap<String, MessageDetails>>,
}

#[derive(Debug, Clone, Default)]
struct MessageDetails {
    committed_offset: u64,
    next_offset: u64,
    messages: Vec<MessageEntry>,
}

#[derive(Debug, Clone)]
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
#[enum_dispatch(HandleMessage)]
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

impl HandleMessage for Send {
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
            payload: _,
        } = body;
        let Self { key, msg } = self;

        let mut msgs = node.messages.lock().await;
        let entry = msgs.entry(key).or_default();

        let offset = entry.next_offset;
        entry.next_offset += 1;
        entry.messages.push(MessageEntry { offset, msg });

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

impl HandleMessage for SendOk {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Poll {
    offsets: HashMap<String, u64>,
}

impl HandleMessage for Poll {
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
            payload: _,
        } = body;
        let Self { offsets } = self;
        let mut result = HashMap::new();
        let msgs = node.messages.lock().await;

        for (key, offset) in offsets {
            if let Some(entries) = msgs.get(&key) {
                let filtered: Vec<PollOkMessageEntry> = entries
                    .messages
                    .iter()
                    .cloned()
                    .filter(|entry| entry.offset >= offset)
                    .map(|entry| PollOkMessageEntry {
                        offset: entry.offset,
                        msg: entry.msg,
                    })
                    .collect();
                if !filtered.is_empty() {
                    result.insert(key, filtered);
                }
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

impl HandleMessage for PollOk {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct CommitOffsets {
    offsets: HashMap<String, u64>,
}

impl HandleMessage for CommitOffsets {
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
            payload: _,
        } = body;
        let Self { offsets } = self;

        let mut msgs = node.messages.lock().await;
        for (key, offset) in offsets {
            let entry = msgs.entry(key).or_default();
            entry.committed_offset = entry.committed_offset.max(offset);
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

impl HandleMessage for CommitOffsetsOk {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct ListCommittedOffsets {
    keys: Vec<String>,
}

impl HandleMessage for ListCommittedOffsets {
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
            payload: _,
        } = body;
        let Self { keys } = self;

        let mut result = HashMap::new();
        let msgs = node.messages.lock().await;
        for key in keys {
            if let Some(entry) = msgs.get(&key) {
                result.insert(key, entry.committed_offset);
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

impl HandleMessage for ListCommittedOffsetsOk {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Error {
    code: u64,
    text: String,
}

impl HandleMessage for Error {
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

#[derive(Debug)]
enum SuppliedPayload {
    Tick,
}

#[enum_dispatch]
trait HandleMessage: Sized {
    async fn handle(
        self,
        msg: Message<Body<()>>,
        node: &Kafka,
        tx: Sender<Message<Body<KafkaNodePayload>>>,
    ) -> anyhow::Result<()> {
        _ = msg;
        _ = node;
        _ = tx;
        bail!("unexpected msg type")
    }
}

impl Node for Kafka {
    type Payload = KafkaNodePayload;
    type PayloadSupplied = SuppliedPayload;
    type Service = ();

    async fn from_init(
        init: Init,
        _service: (),
        id_provider: MsgIDProvider,
        tx: Sender<Self::PayloadSupplied>,
    ) -> anyhow::Result<Self> {
        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();

        tracker.spawn({
            let cancel = cancel.clone();
            async move {
                let mut ticker = tokio::time::interval(Duration::from_millis(100));
                ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

                loop {
                    select! {
                        _ = ticker.tick() => {
                            tx.send(SuppliedPayload::Tick)
                                .await
                                .context("failed to send msg")?;

                        }
                        _ = cancel.cancelled() => {
                            return Ok::<_, anyhow::Error>(())
                        }
                    }
                }
            }
        });

        Ok(Kafka {
            inner: Arc::new(KafkaNodeInner {
                node_id: init.node_id,
                id_provider,
                cancel,
                tasks: tracker,
                messages: Mutex::new(HashMap::new()),
            }),
        })
    }

    async fn handle(
        &self,
        msg: Message<Body<Self::Payload>>,
        tx: Sender<Message<Body<Self::Payload>>>,
    ) -> anyhow::Result<()> {
        let (payload, message) = msg.replace_payload(());

        payload.handle(message, self, tx).await?;

        Ok(())
    }

    async fn handle_supplied(
        &self,
        msg: Self::PayloadSupplied,
        _tx: Sender<Message<Body<Self::Payload>>>,
    ) -> anyhow::Result<()> {
        let SuppliedPayload::Tick = msg;

        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        self.cancel.cancel();
        self.tasks.close();
        self.tasks.wait().await;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop::<Kafka>()
        .await
        .inspect_err(|err| error!("failed to run main: {err}"))
}
