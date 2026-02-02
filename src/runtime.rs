use anyhow::{Context, bail};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;
use std::fmt::Debug;
use std::future::Future;
use std::io::BufRead;
use std::sync::Once;
use tokio::io::AsyncWriteExt;
use tokio::select;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{EnvFilter, fmt};

use crate::kv::{KvClient, KvPayload, MsgIDProvider, StoreName, TsoClient};
use crate::message::{Body, Init, Message, MinBody, PayloadInit};

pub trait Service: Sized + Send + Sync + Clone + 'static {
    fn init(
        id_provider: MsgIDProvider,
        node_id: String,
        tx: Sender<Message<Body<KvPayload>>>,
    ) -> Self;

    fn should_process(&self, reply_id: u64) -> impl Future<Output = anyhow::Result<bool>> + Send;

    fn process(
        &self,
        msg: Message<Body<KvPayload>>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    fn stop(&self) -> impl Future<Output = ()> + Send;
}

impl Service for () {
    fn init(
        _id_provider: MsgIDProvider,
        _node_id: String,
        _tx: Sender<Message<Body<KvPayload>>>,
    ) -> Self {
    }

    async fn should_process(&self, _reply_id: u64) -> anyhow::Result<bool> {
        Ok(false)
    }

    async fn process(&self, _msg: Message<Body<KvPayload>>) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) {}
}

impl<S: StoreName + Send + Sync + Clone + 'static> Service for KvClient<S> {
    fn init(
        id_provider: MsgIDProvider,
        node_id: String,
        tx: Sender<Message<Body<KvPayload>>>,
    ) -> Self {
        KvClient::<S>::new(id_provider, node_id, tx)
    }

    async fn should_process(&self, reply_id: u64) -> anyhow::Result<bool> {
        self.should_process(reply_id).await
    }

    async fn process(&self, msg: Message<Body<KvPayload>>) -> anyhow::Result<()> {
        self.process(msg).await
    }

    async fn stop(&self) {
        self.stop().await
    }
}

impl Service for TsoClient {
    fn init(
        id_provider: MsgIDProvider,
        node_id: String,
        tx: Sender<Message<Body<KvPayload>>>,
    ) -> Self {
        TsoClient::new(id_provider, node_id, tx)
    }

    async fn should_process(&self, reply_id: u64) -> anyhow::Result<bool> {
        self.should_process(reply_id).await
    }

    async fn process(&self, msg: Message<Body<KvPayload>>) -> anyhow::Result<()> {
        self.process(msg).await
    }

    async fn stop(&self) {
        self.stop().await
    }
}

impl<A: Service> Service for (A,) {
    fn init(
        id_provider: MsgIDProvider,
        node_id: String,
        tx: Sender<Message<Body<KvPayload>>>,
    ) -> Self {
        (A::init(id_provider, node_id, tx),)
    }

    async fn should_process(&self, reply_id: u64) -> anyhow::Result<bool> {
        self.0.should_process(reply_id).await
    }

    async fn process(&self, msg: Message<Body<KvPayload>>) -> anyhow::Result<()> {
        self.0.process(msg).await
    }

    async fn stop(&self) {
        self.0.stop().await
    }
}

impl<A: Service, B: Service> Service for (A, B) {
    fn init(
        id_provider: MsgIDProvider,
        node_id: String,
        tx: Sender<Message<Body<KvPayload>>>,
    ) -> Self {
        (
            A::init(id_provider.clone(), node_id.clone(), tx.clone()),
            B::init(id_provider, node_id, tx),
        )
    }

    async fn should_process(&self, reply_id: u64) -> anyhow::Result<bool> {
        if self.0.should_process(reply_id).await? {
            return Ok(true);
        }
        self.1.should_process(reply_id).await
    }

    async fn process(&self, msg: Message<Body<KvPayload>>) -> anyhow::Result<()> {
        if let Some(reply_id) = msg.body.in_reply_to {
            if self.0.should_process(reply_id).await? {
                return self.0.process(msg).await;
            }
            if self.1.should_process(reply_id).await? {
                return self.1.process(msg).await;
            }
        }
        bail!("neither service wanted to process message")
    }

    async fn stop(&self) {
        self.0.stop().await;
        self.1.stop().await;
    }
}

pub trait Node: Clone + Sized + Send + 'static {
    type Payload: Debug + Serialize + DeserializeOwned + Send + 'static;
    type PayloadSupplied: Debug + Send + 'static;
    type Service: Service;

    fn from_init(
        init: Init,
        services: Self::Service,
        id_provider: MsgIDProvider,
        rx: Sender<Self::PayloadSupplied>,
    ) -> impl Future<Output = anyhow::Result<Self>> + Send;
    fn handle(
        &self,
        msg: Message<Body<Self::Payload>>,
        tx: Sender<Message<Body<Self::Payload>>>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn handle_supplied(
        &self,
        msg: Self::PayloadSupplied,
        tx: Sender<Message<Body<Self::Payload>>>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn stop(&self) -> impl Future<Output = anyhow::Result<()>> + Send;
}

pub async fn main_loop<N: Node>() -> anyhow::Result<()> {
    init_tracing();

    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;

    let mut stdin = spawn_stdin();

    let (tx_node, rx_node) = channel::<Message<Body<N::Payload>>>(1);
    let (tx_kv, rx_kv) = channel::<Message<Body<KvPayload>>>(1);
    let (tx_init, rx_init) = channel::<Message<Body<PayloadInit>>>(1);
    let id_provider = MsgIDProvider::new();

    let writer_handle = spawn_stdout_writer::<N>(rx_node, rx_kv, rx_init);

    let (node, mut rx_node_supplied, services, buffered) = select! {
        _ = futures_lite::future::race(sigint.recv(), sigterm.recv()) => {
            return Ok(())
        }
        res = handle_init::<N>(&mut stdin, id_provider, tx_kv, tx_init) => {
            res?
        }
    };

    let mut set = JoinSet::new();

    for line in buffered {
        if let Err(err) = process_line(line, &services, &node, &tx_node, &mut set).await {
            error!("failed to process buffered init message: {err}");
        }
    }

    let mut supplied_open = true;

    loop {
        select! {
            _ = futures_lite::future::race(sigint.recv(), sigterm.recv()) => {
                return Ok(())
            }
            line = stdin.recv() => {
                let Some(line) = line else {
                    warn!("stdin channel closed");
                    break
                };
                let line = line.context("stdin recv returned error before JSON parsing")?;

                if let Err(err) = process_line(line, &services, &node, &tx_node, &mut set).await {
                     error!("failed to process inbound message: {err}");
                }
            }
            supplied = rx_node_supplied.recv(), if supplied_open => {
                let Some(supplied) = supplied else {
                    supplied_open = false;
                    warn!("supplied channel closed");
                    continue
                };

                let tx = tx_node.clone();
                let node = node.clone();

                set.spawn(async move {
                    debug!("handling supplied msg {supplied:?}");
                    let resp = node.handle_supplied(supplied, tx).await;
                    if let Err(err) = resp {
                        error!("node handle supplied failed: {err}");
                    }
                });

            }
        }
    }

    drop(tx_node);

    while let Some(result) = set.join_next().await {
        if let Err(err) = result {
            error!("node task panicked: {err}");
        }
    }

    info!("about to wait for writer task");
    writer_handle.await.context("writer task panicked")?;
    info!("about to wait for node");
    node.stop().await.context("failed to stop node")?;
    services.stop().await;
    info!("goodbye!");

    Ok(())
}

async fn process_line<N: Node>(
    line: String,
    services: &N::Service,
    node: &N,
    tx_node: &Sender<Message<Body<N::Payload>>>,
    set: &mut JoinSet<()>,
) -> anyhow::Result<()> {
    let msg: Message<&RawValue> = serde_json::from_str(line.as_str())
        .with_context(|| format!("failed to parse inbound message envelope: {line}"))?;
    let min_body: MinBody = serde_json::from_str(msg.body.get()).with_context(|| {
        format!(
            "failed to parse inbound minimal body from payload: {}",
            msg.body.get()
        )
    })?;

    if let Some(reply_id) = min_body.in_reply_to
        && services.should_process(reply_id).await?
    {
        let (bdy, msg) = msg.extract_body();
        let payload: Body<KvPayload> = serde_json::from_str(bdy.get())
            .with_context(|| format!("failed to parse kv payload from raw body: {}", bdy.get()))?;
        let (_, msg) = msg.replace_body(payload);
        debug!("kv processing msg {msg:?}");
        services
            .process(msg)
            .await
            .context("kv failed to process msg")?;
        return Ok(());
    }

    let (bdy, msg) = msg.extract_body();
    let payload: Body<N::Payload> = serde_json::from_str(bdy.get())
        .with_context(|| format!("failed to parse node payload from raw body: {}", bdy.get()))?;
    let (_, msg) = msg.replace_body(payload);

    let tx = tx_node.clone();
    let node = node.clone();

    set.spawn(async move {
        debug!("handling msg {msg:?}");
        let resp = node.handle(msg, tx).await;
        if let Err(err) = resp {
            error!("node handle failed: {err}");
        }
    });

    Ok(())
}

async fn handle_init<N: Node>(
    rx_stdin: &mut Receiver<anyhow::Result<String>>,
    id_provider: MsgIDProvider,
    tx_kv: Sender<Message<Body<KvPayload>>>,
    tx_init: Sender<Message<Body<PayloadInit>>>,
) -> Result<(N, Receiver<N::PayloadSupplied>, N::Service, Vec<String>), anyhow::Error> {
    let line = rx_stdin
        .recv()
        .await
        .context("failed to read from stdin")?
        .context("no line was present for init msg")?;
    let init_msg: Message<Body<PayloadInit>> =
        serde_json::from_str(&line).context("failed to parse init msg")?;
    debug!("received init msg: {init_msg:?}");

    let Message { src, dst, body } = init_msg;
    let Body {
        incoming_msg_id: id,
        in_reply_to: _,
        payload,
    } = body;
    let PayloadInit::Init(init) = payload else {
        bail!("should not receive an InitOk msg first")
    };

    let services = N::Service::init(id_provider.clone(), init.node_id.clone(), tx_kv);

    let (tx_supplied, rx_supplied) = channel(1);

    let services_init = services.clone();
    let mut init_fut = std::pin::pin!(async move {
        N::from_init(init, services_init, id_provider, tx_supplied)
            .await
            .context("failed to build node")
    });

    let services_clone = services.clone();
    let mut buffered_lines = Vec::new();

    let node = loop {
        select! {
            res = &mut init_fut => {
                break res?;
            }
            line = rx_stdin.recv() => {
                let Some(line) = line else {
                    bail!("stdin closed during init");
                };
                let line = line.context("stdin recv error during init")?;

                let msg: Message<&RawValue> = serde_json::from_str(&line)
                    .with_context(|| format!("failed to parse inbound message envelope: {line}"))?;
                let min_body: MinBody = serde_json::from_str(msg.body.get()).with_context(|| {
                    format!(
                        "failed to parse inbound minimal body from payload: {}",
                        msg.body.get()
                    )
                })?;

                if let Some(reply_id) = min_body.in_reply_to
                    && services_clone.should_process(reply_id).await?
                {
                    let (bdy, msg) = msg.extract_body();
                    let payload: Body<KvPayload> =
                        serde_json::from_str(bdy.get()).with_context(|| {
                            format!("failed to parse kv payload from raw body: {}", bdy.get())
                        })?;
                    let (_, msg) = msg.replace_body(payload);
                    debug!("kv processing msg {msg:?}");
                    services_clone
                        .process(msg)
                        .await
                        .context("kv failed to process msg")?;
                } else {
                    warn!("buffering message during init: {line}");
                    buffered_lines.push(line);
                }
            }
        }
    };

    let response = Message {
        src: dst,
        dst: src,
        body: Body {
            incoming_msg_id: Some(0),
            in_reply_to: id,
            payload: PayloadInit::InitOk,
        },
    };

    tx_init
        .send(response)
        .await
        .context("failed to send init response")?;

    Ok((node, rx_supplied, services, buffered_lines))
}

fn spawn_stdout_writer<N: Node>(
    mut rx_node: Receiver<Message<Body<N::Payload>>>,
    mut rx_kv: Receiver<Message<Body<KvPayload>>>,
    mut rx_init: Receiver<Message<Body<PayloadInit>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        let mut node_running = true;
        let mut kv_running = true;
        let mut init_running = true;

        loop {
            select! {
                msg = rx_node.recv(), if node_running => {
                    handle_output(msg, &mut node_running, "node", &mut stdout).await;
                },
                msg = rx_kv.recv(), if kv_running => {
                   handle_output(msg, &mut kv_running, "kv", &mut stdout).await;
                },
                msg = rx_init.recv(), if init_running => {
                   handle_output(msg, &mut init_running, "init", &mut stdout).await;
                },
                else => {
                    break;
                }
            }
        }
    })
}

async fn handle_output<T: Serialize + Debug>(
    msg: Option<T>,
    running: &mut bool,
    ctx: &str,
    stdout: &mut tokio::io::Stdout,
) {
    let msg = match msg {
        Some(msg) => msg,
        None => {
            *running = false;
            return;
        }
    };
    debug!("sending {ctx} msg {msg:?}");
    match serde_json::to_string(&msg) {
        Ok(mut outbound) => {
            outbound.push('\n');

            if let Err(err) = stdout.write_all(outbound.as_bytes()).await {
                error!("failed to write {ctx} msg to stdout: {err}");
            }
        }
        Err(err) => {
            error!("failed to serialize outbound {ctx} msg {msg:?} before writing: {err}");
        }
    }
}

fn spawn_stdin() -> Receiver<anyhow::Result<String>> {
    let (tx, rx) = channel(1);

    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            if let Err(err) = tx.blocking_send(line.context("failed to read line")) {
                warn!("stdin channel closed after send failure: {err}");
                break;
            }
        }
    });

    rx
}

static TRACING_INIT: Once = Once::new();

fn init_tracing() {
    TRACING_INIT.call_once(|| {
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
        let _ = fmt()
            .with_env_filter(env_filter)
            .with_writer(std::io::stderr)
            .with_ansi(false)
            .try_init();
    });
}
