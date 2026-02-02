use anyhow::{Context, bail};
use enum_dispatch::enum_dispatch;
use flyio::{Body, Init, KvClient, Message, MsgIDProvider, Node, main_loop};
use serde::{Deserialize, Serialize};
use std::{
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{select, sync::mpsc::Sender, time::MissedTickBehavior};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, warn};
use tracing_subscriber::{EnvFilter, fmt};

const COUNTER_KEY: &str = "shared_counter";

#[derive(Clone)]
struct GrowNode {
    inner: Arc<GrowNodeInner>,
}

struct GrowNodeInner {
    node_id: String,
    id_provider: MsgIDProvider,
    propogate: AtomicU64,
    kv: KvClient,
    tasks: TaskTracker,
    cancel: CancellationToken,
}

impl GrowNodeInner {
    async fn flush_pending(&self) -> anyhow::Result<()> {
        let pending = self.propogate.swap(0, Ordering::SeqCst);
        if pending == 0 {
            return Ok(());
        }

        let mut sleep_duration = Duration::from_millis(1);
        loop {
            let value = self.kv.read(COUNTER_KEY).await?;
            let new_value = value
                .checked_add(pending)
                .context("shared counter would overflow u64")?;

            match self
                .kv
                .compare_and_swap(COUNTER_KEY, value, new_value, false)
                .await?
            {
                true => {
                    debug!(value, new_value, pending, "flushed pending delta");
                    break;
                }
                false => {
                    debug!(value, new_value, pending, "flush contention; retrying");
                    tokio::time::sleep(sleep_duration).await;
                    sleep_duration = sleep_duration
                        .saturating_mul(2)
                        .min(Duration::from_millis(100));
                }
            }
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
#[enum_dispatch(HandleMessage)]
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

impl HandleMessage for Add {
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
                    payload: _,
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

impl HandleMessage for AddOk {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Read;

impl HandleMessage for Read {
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
                    payload: _,
                },
        } = msg;

        node.flush_pending().await?;

        let sync_key = format!("sync-{}", node.node_id);
        let sync_val = node.id_provider.id();

        node.kv
            .write(sync_key, sync_val)
            .await
            .context("failed to write sync key")?;

        let value = match node.kv.read(COUNTER_KEY).await {
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

impl HandleMessage for ReadOk {}

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
        _node: &GrowNode,
        _tx: Sender<Message<Body<GrowNodePayload>>>,
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
        node: &GrowNode,
        tx: Sender<Message<Body<GrowNodePayload>>>,
    ) -> anyhow::Result<()> {
        _ = msg;
        _ = node;
        _ = tx;
        bail!("unexpected msg type")
    }
}

impl Node for GrowNode {
    type Payload = GrowNodePayload;
    type PayloadSupplied = SuppliedPayload;

    async fn from_init(
        init: Init,
        kv: KvClient,
        id_provider: MsgIDProvider,
        tx: Sender<Self::PayloadSupplied>,
    ) -> anyhow::Result<Self> {
        loop {
            match kv.compare_and_swap(COUNTER_KEY, 0, 0, true).await {
                Ok(_) => break,
                Err(_) => {}
            }
        }

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

        Ok(GrowNode {
            inner: Arc::new(GrowNodeInner {
                node_id: init.node_id,
                id_provider,
                propogate: AtomicU64::new(0),
                kv,
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

        if self.propogate.load(Ordering::SeqCst) > 0 {
            self.flush_pending().await?;
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

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    let _ = fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    main_loop::<GrowNode>()
        .await
        .inspect_err(|err| error!("failed to run main: {err}"))
}
