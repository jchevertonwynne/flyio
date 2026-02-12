use anyhow::{Context, bail};
use async_trait::async_trait;
use flyio::{
    Body, Init, KvClientTimeoutExt, Message, MsgIDProvider, Node, SeqKvClient, Worker, main_loop,
};
use serde::{Deserialize, Serialize};
use std::{
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, warn};

const COUNTER_KEY: &str = "shared_counter";
const KV_TIMEOUT: Duration = Duration::from_millis(250);

#[derive(Clone)]
struct GrowNode {
    inner: Arc<GrowNodeInner>,
}

struct GrowNodeInner {
    node_id: String,
    id_provider: MsgIDProvider,
    propogate: AtomicU64,
    kv: SeqKvClient,
}

impl GrowNodeInner {
    async fn flush_pending(&self) -> anyhow::Result<()> {
        let pending = self.propogate.swap(0, Ordering::SeqCst);
        if pending == 0 {
            return Ok(());
        }

        let mut sleep_duration = Duration::from_millis(1);
        loop {
            let value: u64 = self.kv.read_with_timeout(COUNTER_KEY, KV_TIMEOUT).await?;
            let new_value = value
                .checked_add(pending)
                .context("shared counter would overflow u64")?;

            if self
                .kv
                .compare_and_swap_with_timeout(COUNTER_KEY, value, new_value, false, KV_TIMEOUT)
                .await?
            {
                debug!(value, new_value, pending, "flushed pending delta");
                break;
            }

            debug!(value, new_value, pending, "flush contention; retrying");
            tokio::time::sleep(sleep_duration).await;
            sleep_duration = sleep_duration
                .saturating_mul(2)
                .min(Duration::from_millis(100));
        }

        Ok(())
    }
}

impl Deref for GrowNode {
    type Target = GrowNodeInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum GrowNodePayload {
    Add(Add),
    AddOk(AddOk),
    Read(Read),
    ReadOk(ReadOk),
    Error(Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
struct Add {
    delta: u64,
}

impl Add {
    async fn handle(
        self,
        msg: Message<Body<()>>,
        node: &GrowNode,
        tx: Sender<Message<Body<GrowNodePayload>>>,
    ) -> anyhow::Result<()> {
        let Self { delta: message } = self;
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

        node.propogate.fetch_add(message, Ordering::SeqCst);

        let resp_msg_id = node.id_provider.id();

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

impl AddOk {
    #[allow(clippy::unused_async)]
    async fn handle(
        self,
        _msg: Message<Body<()>>,
        _node: &GrowNode,
        _tx: Sender<Message<Body<GrowNodePayload>>>,
    ) -> anyhow::Result<()> {
        bail!("unexpected AddOk message")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Read;

impl Read {
    async fn handle(
        self,
        msg: Message<Body<()>>,
        node: &GrowNode,
        tx: Sender<Message<Body<GrowNodePayload>>>,
    ) -> anyhow::Result<()> {
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

        node.flush_pending().await?;

        let sync_key = format!("sync-{}", node.node_id);
        let sync_val = node.id_provider.id();

        node.kv
            .write_with_timeout(sync_key.as_str(), sync_val, KV_TIMEOUT)
            .await
            .context("failed to write sync key")?;

        let value = match node
            .kv
            .read_with_timeout::<u64>(COUNTER_KEY, KV_TIMEOUT)
            .await
        {
            Ok(v) => v,
            Err(flyio::KvError::KeyDoesNotExist) => 0,
            Err(e) => return Err(e.into()),
        };

        let resp_msg_id = node.id_provider.id();

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

impl ReadOk {
    #[allow(clippy::unused_async)]
    async fn handle(
        self,
        _msg: Message<Body<()>>,
        _node: &GrowNode,
        _tx: Sender<Message<Body<GrowNodePayload>>>,
    ) -> anyhow::Result<()> {
        bail!("unexpected ReadOk message")
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
        _node: &GrowNode,
        _tx: Sender<Message<Body<GrowNodePayload>>>,
    ) -> anyhow::Result<()> {
        let Self { code, text } = self;
        warn!("error payload recieved: code = {code} text = {text}");

        Ok(())
    }
}

impl GrowNodePayload {
    async fn dispatch(
        self,
        msg: Message<Body<()>>,
        node: &GrowNode,
        tx: Sender<Message<Body<GrowNodePayload>>>,
    ) -> anyhow::Result<()> {
        match self {
            GrowNodePayload::Add(payload) => payload.handle(msg, node, tx).await,
            GrowNodePayload::Read(payload) => payload.handle(msg, node, tx).await,
            GrowNodePayload::Error(payload) => payload.handle(msg, node, tx).await,
            GrowNodePayload::AddOk(payload) => payload.handle(msg, node, tx).await,
            GrowNodePayload::ReadOk(payload) => payload.handle(msg, node, tx).await,
        }
    }
}

#[async_trait]
impl Worker<GrowNodePayload> for GrowNode {
    fn tick_interval(&self) -> Option<Duration> {
        Some(Duration::from_millis(100))
    }

    async fn handle_tick(&self, _tx: Sender<Message<Body<GrowNodePayload>>>) -> anyhow::Result<()> {
        if self.propogate.load(Ordering::SeqCst) > 0 {
            self.flush_pending().await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Node<SeqKvClient, Self> for GrowNode {
    type Payload = GrowNodePayload;

    async fn from_init(
        init: Init,
        kv: SeqKvClient,
        id_provider: MsgIDProvider,
    ) -> anyhow::Result<Self> {
        loop {
            match kv
                .compare_and_swap_with_timeout(COUNTER_KEY, 0, 0, true, KV_TIMEOUT)
                .await
            {
                Ok(_) => break,
                Err(flyio::KvError::KeyDoesNotExist) => {}
                Err(e) => return Err(e).context("failed to init counter"),
            }
        }
        Ok(GrowNode {
            inner: Arc::new(GrowNodeInner {
                node_id: init.node_id,
                id_provider,
                propogate: AtomicU64::new(0),
                kv,
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
    main_loop::<GrowNode, SeqKvClient, GrowNode>()
        .await
        .inspect_err(|err| error!("failed to run main: {err}"))
}
