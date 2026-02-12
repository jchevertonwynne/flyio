use anyhow::{Context, bail};
use async_trait::async_trait;
use flyio::{Body, Init, Message, MessageSender, MsgIDProvider, Node, Worker, main_loop};
use futures::stream::{self, StreamExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque, hash_map::Entry},
    ops::Deref,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
use tracing::{debug, error};

#[derive(Clone)]
struct BroadcastNode {
    inner: Arc<BroadcastNodeInner>,
}

struct BroadcastNodeInner {
    node_id: String,
    id_provider: MsgIDProvider,
    completed: CompletedMessages,
    pending: PendingBuffer,
    neighbours: NeighbourRegistry,
}

const SEEN_COMPLETE_LIMIT: usize = 10_000;

struct CompletedMessages {
    limit: usize,
    inner: Mutex<CompletedStorage>,
}

#[derive(Default)]
struct CompletedStorage {
    seen: HashSet<u64>,
    order: VecDeque<u64>,
}

impl CompletedMessages {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            inner: Mutex::new(CompletedStorage::default()),
        }
    }

    async fn snapshot(&self) -> HashSet<u64> {
        let storage = self.inner.lock().await;
        storage.seen.iter().copied().collect()
    }

    async fn record(&self, message: u64) {
        let mut storage = self.inner.lock().await;
        if !storage.seen.insert(message) {
            return;
        }
        storage.order.push_back(message);
        if storage.order.len() > self.limit
            && let Some(evicted) = storage.order.pop_front()
        {
            storage.seen.remove(&evicted);
        }
    }

    async fn filter_new(&self, candidates: Vec<u64>) -> Vec<u64> {
        let storage = self.inner.lock().await;
        candidates
            .into_iter()
            .filter(|msg| !storage.seen.contains(msg))
            .collect()
    }
}

struct PendingBuffer {
    entries: Mutex<HashMap<u64, String>>,
}

impl PendingBuffer {
    fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    async fn queue_from<I>(
        &self,
        source: &str,
        messages: I,
        completed: &CompletedMessages,
    ) -> QueueStats
    where
        I: IntoIterator<Item = u64>,
    {
        let incoming: Vec<u64> = messages.into_iter().collect();
        if incoming.is_empty() {
            let total = self.entries.lock().await.len();
            return QueueStats::empty(total);
        }

        let filtered = completed.filter_new(incoming).await;
        if filtered.is_empty() {
            let total = self.entries.lock().await.len();
            return QueueStats::empty(total);
        }

        let mut pending = self.entries.lock().await;
        let mut added = 0;
        let mut skipped = 0;
        for msg in filtered {
            match pending.entry(msg) {
                Entry::Vacant(slot) => {
                    slot.insert(source.to_string());
                    added += 1;
                }
                Entry::Occupied(_) => {
                    skipped += 1;
                }
            }
        }

        QueueStats {
            added,
            skipped,
            total_after: pending.len(),
        }
    }

    async fn drain(&self) -> Vec<(u64, String)> {
        let mut pending = self.entries.lock().await;
        pending.drain().collect()
    }

    async fn snapshot(&self) -> HashSet<u64> {
        let pending = self.entries.lock().await;
        pending.keys().copied().collect()
    }
}

struct NeighbourRegistry {
    nodes: Mutex<HashSet<String>>,
}

impl NeighbourRegistry {
    fn new() -> Self {
        Self {
            nodes: Mutex::new(HashSet::new()),
        }
    }

    async fn replace(&self, neighbours: HashSet<String>) -> usize {
        let mut guard = self.nodes.lock().await;
        *guard = neighbours;
        guard.len()
    }

    async fn snapshot(&self) -> HashSet<String> {
        self.nodes.lock().await.clone()
    }
}

struct QueueStats {
    added: usize,
    skipped: usize,
    total_after: usize,
}

impl QueueStats {
    fn empty(total_after: usize) -> Self {
        Self {
            added: 0,
            skipped: 0,
            total_after,
        }
    }
}

use self::handlers::BroadcastNodePayload;

impl BroadcastNodeInner {
    async fn snapshot_messages(&self) -> HashSet<u64> {
        let mut messages = self.completed.snapshot().await;
        messages.extend(self.pending.snapshot().await);
        messages
    }

    async fn queue_pending_from<I>(&self, source: &str, messages: I)
    where
        I: IntoIterator<Item = u64>,
    {
        let stats = self
            .pending
            .queue_from(source, messages, &self.completed)
            .await;
        if stats.added > 0 {
            debug!(
                source,
                added = stats.added,
                skipped = stats.skipped,
                pending = stats.total_after,
                "queued new broadcast messages"
            );
        }
    }

    async fn flush_pending(&self) -> HashMap<String, HashSet<u64>> {
        let neighbours = self.neighbours.snapshot().await;
        if neighbours.is_empty() {
            return HashMap::new();
        }

        let drained = self.pending.drain().await;
        if drained.is_empty() {
            return HashMap::new();
        }

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
                self.completed.record(msg).await;
            }
        }

        debug!(
            targets = need_to_send.len(),
            "prepared broadcast gossip batches"
        );
        need_to_send
    }
}

mod handlers {
    use super::{
        Body, BroadcastNode, Context, Deserialize, HashMap, HashSet, Message, MessageSender,
        Serialize, bail, debug,
    };

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    pub(super) enum BroadcastNodePayload {
        Broadcast(Broadcast),
        BroadcastOk(BroadcastOk),
        Read(Read),
        ReadOk(ReadOk),
        Topology(Topology),
        TopologyOk(TopologyOk),
        SendMin(SendMin),
    }

    impl BroadcastNodePayload {
        pub(super) async fn dispatch<T>(
            self,
            msg: Message<Body<()>>,
            node: &BroadcastNode,
            tx: T,
        ) -> anyhow::Result<()>
        where
            T: MessageSender<BroadcastNodePayload>,
        {
            match self {
                BroadcastNodePayload::Broadcast(payload) => payload.handle(msg, node, tx).await,
                BroadcastNodePayload::BroadcastOk(payload) => payload.handle(msg, node, tx).await,
                BroadcastNodePayload::Read(payload) => payload.handle(msg, node, tx).await,
                BroadcastNodePayload::ReadOk(payload) => payload.handle(msg, node, tx).await,
                BroadcastNodePayload::Topology(payload) => payload.handle(msg, node, tx).await,
                BroadcastNodePayload::TopologyOk(payload) => payload.handle(msg, node, tx).await,
                BroadcastNodePayload::SendMin(payload) => payload.handle(msg, node, tx).await,
            }
        }

        pub(super) fn from_messages(messages: HashSet<u64>) -> Self {
            BroadcastNodePayload::SendMin(SendMin { messages })
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    pub(super) struct Broadcast {
        message: u64,
    }

    impl Broadcast {
        async fn handle<T>(
            self,
            msg: Message<Body<()>>,
            node: &BroadcastNode,
            tx: T,
        ) -> anyhow::Result<()>
        where
            T: MessageSender<BroadcastNodePayload>,
        {
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

            tx.send(response)
                .await
                .context("failed to send broadcast ack")?;

            Ok(())
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub(super) struct BroadcastOk;

    impl BroadcastOk {
        #[allow(clippy::unused_async)]
        async fn handle<T>(
            self,
            _msg: Message<Body<()>>,
            _node: &BroadcastNode,
            _tx: T,
        ) -> anyhow::Result<()>
        where
            T: MessageSender<BroadcastNodePayload>,
        {
            bail!("unexpected BroadcastOk message")
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub(super) struct Read;

    impl Read {
        async fn handle<T>(
            self,
            msg: Message<Body<()>>,
            node: &BroadcastNode,
            tx: T,
        ) -> anyhow::Result<()>
        where
            T: MessageSender<BroadcastNodePayload>,
        {
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

            debug!(count = messages.len(), "serving broadcast read");

            let response = Message {
                src: dst,
                dst: src,
                body: Body {
                    incoming_msg_id: Some(resp_msg_id),
                    in_reply_to: incoming_msg_id,
                    payload: BroadcastNodePayload::ReadOk(ReadOk { messages }),
                },
            };

            tx.send(response)
                .await
                .context("failed to send read response")?;

            Ok(())
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub(super) struct ReadOk {
        messages: HashSet<u64>,
    }

    impl ReadOk {
        #[allow(clippy::unused_async)]
        async fn handle<T>(
            self,
            _msg: Message<Body<()>>,
            _node: &BroadcastNode,
            _tx: T,
        ) -> anyhow::Result<()>
        where
            T: MessageSender<BroadcastNodePayload>,
        {
            bail!("unexpected ReadOk message")
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub(super) struct Topology {
        topology: HashMap<String, HashSet<String>>,
    }

    impl Topology {
        async fn handle<T>(
            self,
            msg: Message<Body<()>>,
            node: &BroadcastNode,
            tx: T,
        ) -> anyhow::Result<()>
        where
            T: MessageSender<BroadcastNodePayload>,
        {
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

            let installed = node.neighbours.replace(neighbours).await;
            debug!(installed, "applied broadcast topology update");

            let response = Message {
                src: dst,
                dst: src,
                body: Body {
                    incoming_msg_id: Some(resp_msg_id),
                    in_reply_to: incoming_msg_id,
                    payload: BroadcastNodePayload::TopologyOk(TopologyOk),
                },
            };

            tx.send(response)
                .await
                .context("failed to send topology ack")?;

            Ok(())
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub(super) struct TopologyOk;
    impl TopologyOk {
        #[allow(clippy::unused_async)]
        async fn handle<T>(
            self,
            _msg: Message<Body<()>>,
            _node: &BroadcastNode,
            _tx: T,
        ) -> anyhow::Result<()>
        where
            T: MessageSender<BroadcastNodePayload>,
        {
            bail!("unexpected TopologyOk message")
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub(super) struct SendMin {
        messages: HashSet<u64>,
    }

    impl SendMin {
        async fn handle<T>(
            self,
            msg: Message<Body<()>>,
            node: &BroadcastNode,
            _tx: T,
        ) -> anyhow::Result<()>
        where
            T: MessageSender<BroadcastNodePayload>,
        {
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

            node.queue_pending_from(&src, messages.into_iter()).await;

            Ok(())
        }
    }
}

impl Deref for BroadcastNode {
    type Target = BroadcastNodeInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl Worker<BroadcastNodePayload> for BroadcastNode {
    fn tick_interval(&self) -> Option<Duration> {
        Some(Duration::from_millis(100))
    }

    async fn handle_tick<T: MessageSender<BroadcastNodePayload>>(&self, tx: T) -> anyhow::Result<()> {
        let plan = self.flush_pending().await;

        let sends = stream::iter(plan)
            .map(|(unseen_neighbour, messages)| {
                let tx = tx.clone();
                let src = self.node_id.clone();
                let msg_id = self.id_provider.id();
                async move {
                    tx.send(Message {
                        src,
                        dst: unseen_neighbour,
                        body: Body {
                            incoming_msg_id: Some(msg_id),
                            in_reply_to: None,
                            payload: BroadcastNodePayload::from_messages(messages),
                        },
                    })
                    .await
                    .context("failed to send supplied msg")
                }
            })
            .buffer_unordered(10)
            .collect::<Vec<_>>()
            .await;

        for result in sends {
            result?;
        }

        Ok(())
    }
}

#[async_trait]
impl Node<(), Self> for BroadcastNode {
    type Payload = BroadcastNodePayload;

    async fn from_init(
        init: Init,
        _service: (),
        id_provider: MsgIDProvider,
    ) -> anyhow::Result<Self> {
        let Init {
            node_id,
            node_ids: _,
        } = init;

        Ok(BroadcastNode {
            inner: Arc::new(BroadcastNodeInner {
                node_id,
                id_provider,
                completed: CompletedMessages::new(SEEN_COMPLETE_LIMIT),
                pending: PendingBuffer::new(),
                neighbours: NeighbourRegistry::new(),
            }),
        })
    }

    async fn handle<T>(
        &self,
        msg: Message<Body<Self::Payload>>,
        tx: T,
    ) -> anyhow::Result<()>
    where
        T: MessageSender<Self::Payload>,
    {
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

        payload.dispatch(msg, self, tx).await
    }

    fn get_worker(&self) -> Option<Self> {
        Some(self.clone())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop::<BroadcastNode, (), BroadcastNode>()
        .await
        .inspect_err(|err| error!("failed to run main: {err}"))
}
