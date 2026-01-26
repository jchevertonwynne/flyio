use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, bail};
use enum_dispatch::enum_dispatch;
use flyio::{Body, Init, Message, Node, main_loop};
use serde::{Deserialize, Serialize};
use tokio::{
    select,
    sync::{Mutex, mpsc::Sender},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[derive(Clone)]
struct BroadcastNode {
    inner: Arc<BroadcastNodeInner>,
}

struct BroadcastNodeInner {
    node_id: String,
    msg_id: AtomicU64,
    seen: Mutex<HashMap<u64, HashSet<String>>>,
    neighbours: Mutex<HashSet<String>>,
    tasks: TaskTracker,
    cancel: CancellationToken,
}

impl Deref for BroadcastNode {
    type Target = BroadcastNodeInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[enum_dispatch(HandleMessage)]
enum BroadcastNodePayload {
    Broadcast(Broadcast),
    BroadcastOk(BroadcastOk),
    Read(Read),
    ReadOk(ReadOk),
    ReadMin(ReadMin),
    ReadMinOk(ReadMinOk),
    Topology(Topology),
    TopologyOk(TopologyOk),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
struct Broadcast {
    message: u64,
}

impl HandleMessage for Broadcast {
    async fn handle(
        self,
        node: &BroadcastNode,
        src: String,
        dst: String,
        incoming_msg_id: Option<u64>,
        tx: Sender<Message<BroadcastNodePayload>>,
    ) -> anyhow::Result<()> {
        let Broadcast { message } = self;
        let resp_msg_id = node.msg_id.fetch_add(1, Ordering::SeqCst);
        node.seen
            .lock()
            .await
            .entry(message)
            .or_default()
            .insert(src.clone());

        let response = Message {
            src: dst,
            dst: src,
            body: Body {
                incoming_msg_id: Some(resp_msg_id),
                in_reply_to: incoming_msg_id,
                payload: BroadcastNodePayload::BroadcastOk(BroadcastOk),
            },
        };

        tx.send(response).await.context("channel closed")?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct BroadcastOk;

impl HandleMessage for BroadcastOk {
    async fn handle(
        self,
        _node: &BroadcastNode,
        _src: String,
        _dst: String,
        _incoming_msg_id: Option<u64>,
        _tx: Sender<Message<BroadcastNodePayload>>,
    ) -> anyhow::Result<()> {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Read;
impl HandleMessage for Read {
    async fn handle(
        self,
        node: &BroadcastNode,
        src: String,
        dst: String,
        incoming_msg_id: Option<u64>,
        tx: Sender<Message<BroadcastNodePayload>>,
    ) -> anyhow::Result<()> {
        let resp_msg_id = node.msg_id.fetch_add(1, Ordering::SeqCst);
        let seen = node.seen.lock().await;
        let messages = seen.keys().copied().collect();

        let response = Message {
            src: dst,
            dst: src,
            body: Body {
                incoming_msg_id: Some(resp_msg_id),
                in_reply_to: incoming_msg_id,
                payload: BroadcastNodePayload::ReadOk(ReadOk { messages }),
            },
        };

        tx.send(response).await.context("channel closed")?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct ReadOk {
    messages: HashSet<u64>,
}
impl HandleMessage for ReadOk {
    async fn handle(
        self,
        node: &BroadcastNode,
        src: String,
        _dst: String,
        _incoming_msg_id: Option<u64>,
        _tx: Sender<Message<BroadcastNodePayload>>,
    ) -> anyhow::Result<()> {
        let ReadOk { messages } = self;
        let mut messages_locked = node.seen.lock().await;
        for msg in messages {
            messages_locked.entry(msg).or_default().insert(src.clone());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct ReadMin;
impl HandleMessage for ReadMin {
    async fn handle(
        self,
        node: &BroadcastNode,
        src: String,
        dst: String,
        incoming_msg_id: Option<u64>,
        tx: Sender<Message<BroadcastNodePayload>>,
    ) -> anyhow::Result<()> {
        let Self {} = self;
        let resp_msg_id = node.msg_id.fetch_add(1, Ordering::SeqCst);
        let mut seen = node.seen.lock().await;
        let messages: HashSet<u64> = seen
            .iter()
            .filter_map(|(k, v)| {
                if v.contains(&src) {
                    return None;
                }
                Some(*k)
            })
            .collect();

        if messages.is_empty() {
            return Ok(());
        }

        for msg in &messages {
            if let Some(s) = seen.get_mut(msg) {
                s.insert(src.clone());
            }
        }

        let response = Message {
            src: dst,
            dst: src,
            body: Body {
                incoming_msg_id: Some(resp_msg_id),
                in_reply_to: incoming_msg_id,
                payload: BroadcastNodePayload::ReadMinOk(ReadMinOk { messages }),
            },
        };

        tx.send(response).await.context("channel closed")?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct ReadMinOk {
    messages: HashSet<u64>,
}
impl HandleMessage for ReadMinOk {
    async fn handle(
        self,
        node: &BroadcastNode,
        src: String,
        _dst: String,
        _incoming_msg_id: Option<u64>,
        _tx: Sender<Message<BroadcastNodePayload>>,
    ) -> anyhow::Result<()> {
        let Self { messages } = self;
        let mut messages_locked = node.seen.lock().await;
        for msg in messages {
            messages_locked.entry(msg).or_default().insert(src.clone());
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Topology {
    topology: HashMap<String, HashSet<String>>,
}
impl HandleMessage for Topology {
    async fn handle(
        self,
        node: &BroadcastNode,
        src: String,
        dst: String,
        incoming_msg_id: Option<u64>,
        tx: Sender<Message<BroadcastNodePayload>>,
    ) -> anyhow::Result<()> {
        let Self { mut topology } = self;
        let resp_msg_id = node.msg_id.fetch_add(1, Ordering::SeqCst);
        let Some(neighbours) = topology.remove(&node.node_id) else {
            bail!("malformed topology msg");
        };

        let mut neighbours_locked = node.neighbours.lock().await;
        for neighbour in neighbours {
            neighbours_locked.insert(neighbour);
        }

        let response = Message {
            src: dst,
            dst: src,
            body: Body {
                incoming_msg_id: Some(resp_msg_id),
                in_reply_to: incoming_msg_id,
                payload: BroadcastNodePayload::TopologyOk(TopologyOk),
            },
        };

        tx.send(response).await.context("channel closed")?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct TopologyOk;
impl HandleMessage for TopologyOk {
    async fn handle(
        self,
        _node: &BroadcastNode,
        _src: String,
        _dst: String,
        _incoming_msg_id: Option<u64>,
        _tx: Sender<Message<BroadcastNodePayload>>,
    ) -> anyhow::Result<()> {
        bail!("unexpcted topology ok msg")
    }
}

#[derive(Debug)]
enum SuppliedPayload {
    Gossip,
}

#[enum_dispatch]
trait HandleMessage {
    async fn handle(
        self,
        node: &BroadcastNode,
        src: String,
        dst: String,
        incoming_msg_id: Option<u64>,
        tx: Sender<Message<BroadcastNodePayload>>,
    ) -> anyhow::Result<()>;
}

impl Node for BroadcastNode {
    type Payload = BroadcastNodePayload;
    type PayloadSupplied = SuppliedPayload;

    fn from_init(init: Init, tx: Sender<Self::PayloadSupplied>) -> anyhow::Result<Self> {
        let Init {
            node_id,
            node_ids: _,
        } = init;

        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();

        tracker.spawn({
            let cancel = cancel.clone();
            async move {
                let mut ticker = tokio::time::interval(Duration::from_millis(100));
                loop {
                    select! {
                        _ = ticker.tick() => {
                            tx.send(SuppliedPayload::Gossip)
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

        Ok(BroadcastNode {
            inner: Arc::new(BroadcastNodeInner {
                node_id,
                msg_id: AtomicU64::new(1),
                seen: Mutex::new(HashMap::new()),
                neighbours: Mutex::new(HashSet::new()),
                tasks: tracker,
                cancel,
            }),
        })
    }

    async fn handle(
        &self,
        msg: Message<Self::Payload>,
        tx: Sender<Message<Self::Payload>>,
    ) -> anyhow::Result<()> {
        let Message {
            src,
            dst,
            body:
                Body {
                    incoming_msg_id,
                    in_reply_to: _,
                    payload,
                },
        } = msg;

        payload.handle(self, src, dst, incoming_msg_id, tx).await?;

        Ok(())
    }

    async fn handle_supplied(
        &self,
        msg: Self::PayloadSupplied,
        tx: Sender<Message<Self::Payload>>,
    ) -> anyhow::Result<()> {
        let SuppliedPayload::Gossip = msg;
        for neighbour in self.neighbours.lock().await.iter() {
            tx.send(Message {
                src: self.node_id.clone(),
                dst: neighbour.clone(),
                body: Body {
                    incoming_msg_id: None,
                    in_reply_to: None,
                    payload: BroadcastNodePayload::ReadMin(ReadMin),
                },
            })
            .await
            .context("channel was closed")?;
        }

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
    main_loop::<BroadcastNode>().await
}
