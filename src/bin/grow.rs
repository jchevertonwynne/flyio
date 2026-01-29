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
use tokio::{
    select,
    sync::{OnceCell, mpsc::Sender},
    time::MissedTickBehavior,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, warn};
use tracing_subscriber::{EnvFilter, fmt};

const COUNTER_KEY: &str = "shared_counter";

#[derive(Clone)]
struct GrowNode {
    inner: Arc<GrowNodeInner>,
}

struct GrowNodeInner {
    id_provider: MsgIDProvider,
    propogate: AtomicU64,
    kv: KvClient,
    tasks: TaskTracker,
    cancel: CancellationToken,
    counter_ready: OnceCell<()>,
}

impl GrowNodeInner {
    async fn ensure_counter_ready(&self) -> anyhow::Result<()> {
        self.counter_ready
            .get_or_try_init(|| async {
                self.ensure_counter_exists().await?;
                Ok(())
            })
            .await
            .map(|_| ())
    }

    async fn ensure_counter_exists(&self) -> anyhow::Result<()> {
        let mut sleep_dur = Duration::from_millis(1);
        loop {
            match self.kv.compare_and_swap(COUNTER_KEY, 0, 0, true).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    warn!(?err, "failed to ensure shared counter exists");
                }
            }

            tokio::time::sleep(sleep_dur).await;
            sleep_dur = sleep_dur.saturating_mul(2).min(Duration::from_millis(100));
        }
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

        node.ensure_counter_ready().await?;
        let mut sleep_duration = Duration::from_millis(1);

        let value = loop {
            let value = node.kv.read(COUNTER_KEY).await?;
            match node
                .kv
                .compare_and_swap(COUNTER_KEY, value, value, false)
                .await?
            {
                true => {
                    debug!(value, "successful read of shared counter");
                    break value;
                }
                false => {
                    debug!(value, "retrying read of shared counter after contention");
                    tokio::time::sleep(sleep_duration).await;
                    sleep_duration = sleep_duration
                        .saturating_mul(2)
                        .min(Duration::from_millis(100));
                }
            }
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
        _init: Init,
        kv: KvClient,
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

        Ok(GrowNode {
            inner: Arc::new(GrowNodeInner {
                id_provider,
                propogate: AtomicU64::new(0),
                kv,
                tasks: tracker,
                cancel,
                counter_ready: OnceCell::new(),
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

        let pending = self.propogate.load(Ordering::SeqCst);
        if pending > 0 {
            self.ensure_counter_ready().await?;
            debug!(pending, "flushing shared counter");
            let mut value;
            let mut sleep_duration = Duration::from_millis(1);
            loop {
                value = self.kv.read(COUNTER_KEY).await?;
                let new_value = value + pending;
                match self
                    .kv
                    .compare_and_swap(COUNTER_KEY, value, new_value, false)
                    .await?
                {
                    true => {
                        debug!(value, new_value, "flushed shared counter successfully");
                        self.propogate.store(0, Ordering::SeqCst);
                        break;
                    }
                    false => {
                        debug!(
                            value,
                            "contention detected during flush of shared counter, sleeping for {sleep_duration:?}"
                        );
                        tokio::time::sleep(sleep_duration).await;
                        sleep_duration = sleep_duration
                            .saturating_mul(2)
                            .min(Duration::from_millis(100));
                        continue;
                    }
                }
            }
        }

        self.kv.compare_and_swap("other key", 0, 0, true).await?;

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
        .try_init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    main_loop::<GrowNode>()
        .await
        .inspect_err(|err| error!("failed to run main: {err}"))
}
