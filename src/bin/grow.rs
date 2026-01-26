use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, bail};
use enum_dispatch::enum_dispatch;
use flyio::{Body, Init, Message, Node, main_loop};
use lru::LruCache;
use serde::{Deserialize, Serialize};
use tokio::{
    select,
    sync::{Mutex, mpsc::Sender},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

#[derive(Clone)]
struct GrowNode {
    inner: Arc<GrowNodeInner>,
}

struct GrowNodeInner {
    node_id: String,
    neighbours: HashSet<String>,
    msg_id: AtomicU64,
    number: AtomicU64,
    propogate: AtomicU64,
    waiting_list: Mutex<HashMap<Uuid, WaitingListRecord>>,
    seen: Mutex<LruCache<Uuid, ()>>,
    tasks: TaskTracker,
    cancel: CancellationToken,
}

impl Deref for GrowNode {
    type Target = GrowNodeInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

struct WaitingListRecord {
    delta: u64,
    last_sent: Instant,
    waiting_for: HashSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[enum_dispatch(HandleMessage)]
enum GrowNodePayload {
    Add(Add),
    AddOk(AddOk),
    Read(Read),
    ReadOk(ReadOk),
    PropogateCrawl(PropogateCrawl),
    PropogateCrawlOk(PropogateCrawlOk),
    Error(Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
struct Add {
    delta: u64,
}

impl HandleMessage for Add {
    async fn handle(
        self,
        msg: Message<()>,
        node: &GrowNode,
        tx: Sender<Message<GrowNodePayload>>,
    ) -> anyhow::Result<()> {
        let Self { delta: message } = self;
        let Message {
            src,
            dst,
            body:
                Body {
                    incoming_msg_id,
                    in_reply_to: _,
                    payload: _,
                },
        } = msg;

        let resp_msg_id = node.msg_id.fetch_add(1, Ordering::SeqCst);

        node.number.fetch_add(message, Ordering::SeqCst);
        node.propogate.fetch_add(message, Ordering::SeqCst);

        let response = Message {
            src: dst,
            dst: src,
            body: Body {
                incoming_msg_id: Some(resp_msg_id),
                in_reply_to: incoming_msg_id,
                payload: GrowNodePayload::AddOk(AddOk),
            },
        };

        tx.send(response).await.context("channel closed")?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct AddOk;

impl HandleMessage for AddOk {
    async fn handle(
        self,
        _msg: Message<()>,
        _txde: &GrowNode,
        _tx: Sender<Message<GrowNodePayload>>,
    ) -> anyhow::Result<()> {
        bail!("expected broadcast_ok message recevied")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Read;

impl HandleMessage for Read {
    async fn handle(
        self,
        msg: Message<()>,
        node: &GrowNode,
        tx: Sender<Message<GrowNodePayload>>,
    ) -> anyhow::Result<()> {
        let Message {
            src,
            dst,
            body:
                Body {
                    incoming_msg_id,
                    in_reply_to: _,
                    payload: _,
                },
        } = msg;

        let resp_msg_id = node.msg_id.fetch_add(1, Ordering::SeqCst);
        let value = node.number.load(Ordering::SeqCst);

        let response = Message {
            src: dst,
            dst: src,
            body: Body {
                incoming_msg_id: Some(resp_msg_id),
                in_reply_to: incoming_msg_id,
                payload: GrowNodePayload::ReadOk(ReadOk { value }),
            },
        };

        tx.send(response).await.context("channel closed")?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct ReadOk {
    value: u64,
}

impl HandleMessage for ReadOk {
    async fn handle(
        self,
        _msg: Message<()>,
        _node: &GrowNode,
        _tx: Sender<Message<GrowNodePayload>>,
    ) -> anyhow::Result<()> {
        bail!("should never receive a read_ok message")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct PropogateCrawl {
    crawl_uuid: Uuid,
    source: String,
    target: String,
    delta: u64,
    trail: Vec<String>,
    next: HashSet<String>,
}

impl HandleMessage for PropogateCrawl {
    async fn handle(
        self,
        msg: Message<()>,
        node: &GrowNode,
        tx: Sender<Message<GrowNodePayload>>,
    ) -> anyhow::Result<()> {
        let Self {
            crawl_uuid,
            source,
            target,
            delta,
            mut trail,
            next,
        } = self;
        let Message {
            src: _,
            dst: _,
            body:
                Body {
                    incoming_msg_id,
                    in_reply_to: _,
                    payload: _,
                },
        } = msg;

        if node.node_id == target && node.seen.lock().await.put(crawl_uuid, ()).is_none() {
            node.number.fetch_add(delta, Ordering::SeqCst);
            for neighbour in node.neighbours.iter().cloned() {
                let msg_id = node.msg_id.fetch_add(1, Ordering::SeqCst);
                let mut next = node.neighbours.clone();
                next.retain(|n| n != &node.node_id);
                let msg = Message {
                    src: node.node_id.clone(),
                    dst: neighbour,
                    body: Body {
                        incoming_msg_id: Some(msg_id),
                        in_reply_to: incoming_msg_id,
                        payload: GrowNodePayload::PropogateCrawlOk(PropogateCrawlOk {
                            crawl_uuid,
                            source: node.node_id.clone(),
                            target: target.clone(),
                            next,
                        }),
                    },
                };
                tx.send(msg).await.context("failed to send msg")?;
            }
            return Ok(());
        }

        trail.push(node.node_id.clone());

        for next_neighbour in next.iter().cloned() {
            let msg_id = node.msg_id.fetch_add(1, Ordering::SeqCst);

            let trail = trail.clone();
            let mut next = next.clone();
            next.retain(|id| id != &node.node_id);

            let msg = Message {
                src: node.node_id.clone(),
                dst: next_neighbour,
                body: Body {
                    incoming_msg_id: Some(msg_id),
                    in_reply_to: incoming_msg_id,
                    payload: GrowNodePayload::PropogateCrawl(PropogateCrawl {
                        crawl_uuid,
                        source: node.node_id.clone(),
                        target: source.clone(),
                        delta,
                        trail,
                        next,
                    }),
                },
            };
            tx.send(msg)
                .await
                .context("failed to send propogate message")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct PropogateCrawlOk {
    crawl_uuid: Uuid,
    source: String,
    target: String,
    next: HashSet<String>,
}

impl HandleMessage for PropogateCrawlOk {
    async fn handle(
        self,
        _msg: Message<()>,
        node: &GrowNode,
        tx: Sender<Message<GrowNodePayload>>,
    ) -> anyhow::Result<()> {
        let Self {
            crawl_uuid,
            source,
            target,
            next,
        } = self;

        if target == node.node_id {
            let mut waiting_list = node.waiting_list.lock().await;
            let Some(record) = waiting_list.get_mut(&crawl_uuid) else {
                return Ok(());
            };
            record.waiting_for.remove(&source);
            // remove from map
            return Ok(());
        }

        for dst in next.iter().cloned() {
            let mut next = next.clone();
            next.remove(&dst);
            let msg_id = node.msg_id.fetch_add(1, Ordering::SeqCst);
            let message = Message {
                src: node.node_id.clone(),
                dst,
                body: Body {
                    incoming_msg_id: Some(msg_id),
                    in_reply_to: None,
                    payload: GrowNodePayload::PropogateCrawlOk(PropogateCrawlOk {
                        crawl_uuid,
                        source: source.clone(),
                        target: target.clone(),
                        next,
                    }),
                },
            };
            tx.send(message).await.context("failed to send msg")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Error {
    code: u64,
    text: String,
}

impl HandleMessage for Error {
    async fn handle(
        self,
        _msg: Message<()>,
        _node: &GrowNode,
        _tx: Sender<Message<GrowNodePayload>>,
    ) -> anyhow::Result<()> {
        let Self { code, text } = self;
        eprintln!("error payload recieved: code = {code} text = {text}");

        Ok(())
    }
}

#[derive(Debug)]
enum SuppliedPayload {
    Tick,
}

#[enum_dispatch]
trait HandleMessage {
    async fn handle(
        self,
        msg: Message<()>,
        node: &GrowNode,
        tx: Sender<Message<GrowNodePayload>>,
    ) -> anyhow::Result<()>;
}

impl Node for GrowNode {
    type Payload = GrowNodePayload;
    type PayloadSupplied = SuppliedPayload;

    fn from_init(init: Init, tx: Sender<Self::PayloadSupplied>) -> anyhow::Result<Self> {
        let Init { node_id, node_ids } = init;

        let mut neighbours = node_ids;
        neighbours.retain(|n| n != &node_id);

        let cancel = CancellationToken::new();
        let tracker = TaskTracker::new();

        tracker.spawn({
            let cancel = cancel.clone();
            async move {
                let mut ticker = tokio::time::interval(Duration::from_millis(100));
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

        Ok(GrowNode {
            inner: Arc::new(GrowNodeInner {
                node_id,
                neighbours,
                msg_id: AtomicU64::new(1),
                number: AtomicU64::new(0),
                propogate: AtomicU64::new(0),
                waiting_list: Mutex::new(HashMap::new()),
                seen: Mutex::new(LruCache::new(
                    NonZeroUsize::new(1024).context("NZU invalud")?,
                )),
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
        tx: Sender<Message<Self::Payload>>,
    ) -> anyhow::Result<()> {
        let SuppliedPayload::Tick = msg;

        for (&crawl_uuid, record) in self.waiting_list.lock().await.iter_mut() {
            if record.last_sent.elapsed() > Duration::from_secs(1) {
                let delta = record.delta;
                eprintln!(
                    "retrying to send delta {delta} to neighbours {:?}",
                    record.waiting_for
                );

                for neighbour in record.waiting_for.iter().cloned() {
                    let msg_id = self.msg_id.fetch_add(1, Ordering::SeqCst);
                    tx.send(Message {
                        src: self.node_id.clone(),
                        dst: neighbour.clone(),
                        body: Body {
                            incoming_msg_id: Some(msg_id),
                            in_reply_to: None,
                            payload: GrowNodePayload::PropogateCrawl(PropogateCrawl {
                                crawl_uuid,
                                source: self.node_id.clone(),
                                target: neighbour,
                                delta,
                                trail: vec![self.node_id.clone()],
                                next: self.neighbours.clone(),
                            }),
                        },
                    })
                    .await
                    .context("failed to send supplied msg")?;
                }

                record.last_sent = Instant::now();
            }
        }

        let delta = self.propogate.swap(0, Ordering::SeqCst);
        if delta == 0 {
            return Ok(());
        }

        let crawl_uuid = Uuid::new_v4();
        for target in self.neighbours.iter().cloned() {
            let msg_id = self.msg_id.fetch_add(1, Ordering::SeqCst);
            let trail = vec![self.node_id.clone()];
            let next = self.neighbours.clone();
            tx.send(Message {
                src: self.node_id.clone(),
                dst: target.clone(),
                body: Body {
                    incoming_msg_id: Some(msg_id),
                    in_reply_to: None,
                    payload: GrowNodePayload::PropogateCrawl(PropogateCrawl {
                        crawl_uuid,
                        source: self.node_id.clone(),
                        target,
                        delta,
                        trail,
                        next,
                    }),
                },
            })
            .await
            .context("failed to send supplied msg")?;
        }

        let record = WaitingListRecord {
            delta,
            last_sent: Instant::now(),
            waiting_for: self.neighbours.clone(),
        };
        self.waiting_list.lock().await.insert(crawl_uuid, record);

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
    main_loop::<GrowNode>().await
}
