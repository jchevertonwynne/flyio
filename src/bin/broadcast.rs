use anyhow::{Context, bail};
use enum_dispatch::enum_dispatch;
use flyio::{Body, Init, Message, MsgIDProvider, Node, main_loop};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    ops::Deref,
    sync::Arc,
    time::Duration,
};
use tokio::{
    select,
    sync::{Mutex, mpsc::Sender},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::error;

#[derive(Clone)]
struct BroadcastNode {
    inner: Arc<BroadcastNodeInner>,
}

struct BroadcastNodeInner {
    node_id: String,
    id_provider: MsgIDProvider,
    seen_complete: Mutex<HashSet<u64>>,
    completed_order: Mutex<VecDeque<u64>>,
    seen: Mutex<HashMap<u64, String>>,
    neighbours: Mutex<HashSet<String>>,
    tasks: TaskTracker,
    cancel: CancellationToken,
}

const SEEN_COMPLETE_LIMIT: usize = 10_000;

impl BroadcastNodeInner {
    async fn snapshot_messages(&self) -> HashSet<u64> {
        let mut messages = HashSet::new();
        {
            let complete = self.seen_complete.lock().await;
            messages.reserve(complete.len());
            messages.extend(complete.iter().copied());
        }
        {
            let seen = self.seen.lock().await;
            messages.reserve(seen.len());
            messages.extend(seen.keys().copied());
        }
        messages
    }

    async fn record_completion(&self, message: u64) {
        {
            let mut complete = self.seen_complete.lock().await;
            if !complete.insert(message) {
                return;
            }
        }

        let mut evicted = None;
        {
            let mut order = self.completed_order.lock().await;
            order.push_back(message);
            if order.len() > SEEN_COMPLETE_LIMIT {
                evicted = order.pop_front();
            }
        }

        if let Some(evicted_id) = evicted {
            let mut complete = self.seen_complete.lock().await;
            complete.remove(&evicted_id);
        }
    }

    async fn queue_pending_from<I>(&self, source: &str, messages: I)
    where
        I: IntoIterator<Item = u64>,
    {
        let mut pending: Vec<u64> = messages.into_iter().collect();
        if pending.is_empty() {
            return;
        }

        let filtered: Vec<u64> = {
            let complete = self.seen_complete.lock().await;
            pending.retain(|msg| !complete.contains(msg));
            pending
        };

        if filtered.is_empty() {
            return;
        }

        let mut seen_locked = self.seen.lock().await;
        for msg in filtered {
            seen_locked.entry(msg).or_insert_with(|| source.to_string());
        }
    }

    async fn flush_pending(&self) -> HashMap<String, HashSet<u64>> {
        let neighbours = {
            let neighbours = self.neighbours.lock().await;
            neighbours.clone()
        };

        let drained: Vec<(u64, String)> = {
            let mut msgs = self.seen.lock().await;
            msgs.drain().collect()
        };

        let grouped: HashMap<String, Vec<u64>> = drained
            .into_iter()
            .map(|(msg, seen_by)| (seen_by, msg))
            .into_group_map();

        let mut need_to_send: HashMap<String, HashSet<u64>> = HashMap::new();

        for (seen_by, msgs) in grouped {
            for neighbour in neighbours.iter().filter(|n| *n != &seen_by) {
                need_to_send
                    .entry(neighbour.clone())
                    .or_default()
                    .extend(msgs.iter().copied());
            }
            for msg in msgs {
                self.record_completion(msg).await;
            }
        }

        need_to_send
    }
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
    Topology(Topology),
    TopologyOk(TopologyOk),
    SendMin(SendMin),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
struct Broadcast {
    message: u64,
}

impl HandleMessage for Broadcast {
    async fn handle(
        self,
        msg: Message<Body<()>>,
        node: &BroadcastNode,
        tx: Sender<Message<Body<BroadcastNodePayload>>>,
    ) -> anyhow::Result<()> {
        let Self { message } = self;
        let Message {
            src,
            dst,
            body:
                Body {
                    incoming_msg_id,
                    in_reply_to: _,
                    payload: (),
                },
        } = msg;

        let resp_msg_id = node.id_provider.id();
        node.queue_pending_from(&src, [message]).await;

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

impl HandleMessage for BroadcastOk {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Read;

impl HandleMessage for Read {
    async fn handle(
        self,
        msg: Message<Body<()>>,
        node: &BroadcastNode,
        tx: Sender<Message<Body<BroadcastNodePayload>>>,
    ) -> anyhow::Result<()> {
        let Self {} = self;
        let Message {
            src,
            dst,
            body:
                Body {
                    incoming_msg_id,
                    in_reply_to: _,
                    payload: (),
                },
        } = msg;

        let resp_msg_id = node.id_provider.id();
        let messages = node.snapshot_messages().await;

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

impl HandleMessage for ReadOk {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Topology {
    topology: HashMap<String, HashSet<String>>,
}

impl HandleMessage for Topology {
    async fn handle(
        self,
        msg: Message<Body<()>>,
        node: &BroadcastNode,
        tx: Sender<Message<Body<BroadcastNodePayload>>>,
    ) -> anyhow::Result<()> {
        let Self { mut topology } = self;
        let Message {
            src,
            dst,
            body:
                Body {
                    incoming_msg_id,
                    in_reply_to: _,
                    payload: (),
                },
        } = msg;

        let resp_msg_id = node.id_provider.id();
        let Some(neighbours) = topology.remove(&node.node_id) else {
            bail!("malformed topology msg");
        };

        {
            let mut neighbours_locked = node.neighbours.lock().await;
            for neighbour in neighbours {
                neighbours_locked.insert(neighbour);
            }
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
impl HandleMessage for TopologyOk {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct SendMin {
    messages: HashSet<u64>,
}

impl HandleMessage for SendMin {
    async fn handle(
        self,
        msg: Message<Body<()>>,
        node: &BroadcastNode,
        _tx: Sender<Message<Body<BroadcastNodePayload>>>,
    ) -> anyhow::Result<()> {
        let Self { messages } = self;
        let Message {
            src,
            dst: _,
            body:
                Body {
                    incoming_msg_id: _,
                    in_reply_to: _,
                    payload: (),
                },
        } = msg;

        node.queue_pending_from(&src, messages).await;

        Ok(())
    }
}

#[derive(Debug)]
enum SuppliedPayload {
    Gossip,
}

#[enum_dispatch]
trait HandleMessage: Sized {
    async fn handle(
        self,
        msg: Message<Body<()>>,
        node: &BroadcastNode,
        tx: Sender<Message<Body<BroadcastNodePayload>>>,
    ) -> anyhow::Result<()> {
        _ = msg;
        _ = node;
        _ = tx;
        bail!("unexpected msg type")
    }
}

async fn gossip_ticker_loop(
    cancel: CancellationToken,
    tx: Sender<SuppliedPayload>,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    loop {
        select! {
            _ = ticker.tick() => {
                tx.send(SuppliedPayload::Gossip)
                    .await
                    .context("failed to send msg")?;
            }
            () = cancel.cancelled() => {
                return Ok(())
            }
        }
    }
}

impl Node for BroadcastNode {
    type Payload = BroadcastNodePayload;
    type PayloadSupplied = SuppliedPayload;
    type Service = ();

    async fn from_init(
        init: Init,
        _services: (),
        id_provider: MsgIDProvider,
        tx: Sender<Self::PayloadSupplied>,
    ) -> anyhow::Result<Self> {
        let Init {
            node_id,
            node_ids: _,
        } = init;

        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();

        tracker.spawn(gossip_ticker_loop(cancel.clone(), tx.clone()));

        Ok(BroadcastNode {
            inner: Arc::new(BroadcastNodeInner {
                node_id,
                id_provider,
                completed_order: Mutex::new(VecDeque::new()),
                seen_complete: Mutex::new(HashSet::new()),
                seen: Mutex::new(HashMap::new()),
                neighbours: Mutex::new(HashSet::new()),
                tasks: tracker,
                cancel,
            }),
        })
    }

    async fn handle(
        &self,
        msg: Message<Body<Self::Payload>>,
        tx: Sender<Message<Body<Self::Payload>>>,
    ) -> anyhow::Result<()> {
        let Message {
            src,
            dst,
            body:
                Body {
                    incoming_msg_id,
                    in_reply_to,
                    payload,
                },
        } = msg;

        let msg = Message {
            src,
            dst,
            body: Body {
                incoming_msg_id,
                in_reply_to,
                payload: (),
            },
        };

        payload.handle(msg, self, tx).await?;

        Ok(())
    }

    async fn handle_supplied(
        &self,
        msg: Self::PayloadSupplied,
        tx: Sender<Message<Body<Self::Payload>>>,
    ) -> anyhow::Result<()> {
        let SuppliedPayload::Gossip = msg;

        let plan = self.flush_pending().await;

        for (unseen_neighbour, messages) in plan {
            let msg_id = self.id_provider.id();
            tx.send(Message {
                src: self.node_id.clone(),
                dst: unseen_neighbour,
                body: Body {
                    incoming_msg_id: Some(msg_id),
                    in_reply_to: None,
                    payload: BroadcastNodePayload::SendMin(SendMin { messages }),
                },
            })
            .await
            .context("failed to send supplied msg")?;
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
    main_loop::<BroadcastNode>()
        .await
        .inspect_err(|err| error!("failed to run main: {err}"))
}
