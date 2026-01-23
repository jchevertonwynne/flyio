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
enum BroadcastPayload {
    Broadcast {
        message: u64,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<u64>,
    },
    ReadMin,
    ReadMinOk {
        messages: HashSet<u64>,
    },
    Topology {
        topology: HashMap<String, HashSet<String>>,
    },
    TopologyOk,
}

#[derive(Debug)]
enum SuppliedPayload {
    Gossip,
}

impl Node for BroadcastNode {
    type Payload = BroadcastPayload;
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

        match payload {
            BroadcastPayload::Broadcast { message } => {
                let resp_msg_id = self.msg_id.fetch_add(1, Ordering::SeqCst);
                self.seen
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
                        payload: BroadcastPayload::BroadcastOk,
                    },
                };

                tx.send(response).await.context("channel closed")?;
            }
            BroadcastPayload::BroadcastOk => todo!(),
            BroadcastPayload::Read => {
                let resp_msg_id = self.msg_id.fetch_add(1, Ordering::SeqCst);
                let seen = self.seen.lock().await;
                let messages = seen.keys().copied().collect();

                let response = Message {
                    src: dst,
                    dst: src,
                    body: Body {
                        incoming_msg_id: Some(resp_msg_id),
                        in_reply_to: incoming_msg_id,
                        payload: BroadcastPayload::ReadOk { messages },
                    },
                };

                tx.send(response).await.context("channel closed")?;
            }
            BroadcastPayload::ReadOk { messages } => {
                let mut messages_locked = self.seen.lock().await;
                for msg in messages {
                    messages_locked.entry(msg).or_default();
                }
            }
            BroadcastPayload::Topology { mut topology } => {
                let resp_msg_id = self.msg_id.fetch_add(1, Ordering::SeqCst);
                let Some(neighbours) = topology.remove(&self.node_id) else {
                    bail!("malformed topology msg");
                };

                let mut neighbours_locked = self.neighbours.lock().await;
                for neighbour in neighbours {
                    neighbours_locked.insert(neighbour);
                }

                let response = Message {
                    src: dst,
                    dst: src,
                    body: Body {
                        incoming_msg_id: Some(resp_msg_id),
                        in_reply_to: incoming_msg_id,
                        payload: BroadcastPayload::TopologyOk,
                    },
                };

                tx.send(response).await.context("channel closed")?;
            }
            BroadcastPayload::TopologyOk => {}
            BroadcastPayload::ReadMin => {
                let resp_msg_id = self.msg_id.fetch_add(1, Ordering::SeqCst);
                let mut seen = self.seen.lock().await;
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
                        payload: BroadcastPayload::ReadMinOk { messages },
                    },
                };

                tx.send(response).await.context("channel closed")?;
            }
            BroadcastPayload::ReadMinOk { messages } => {
                let mut messages_locked = self.seen.lock().await;
                for msg in messages {
                    messages_locked.entry(msg).or_default();
                }
            }
        };
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
                    payload: BroadcastPayload::ReadMin,
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
