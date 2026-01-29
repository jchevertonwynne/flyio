use anyhow::{Context, bail};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;
use std::fmt::Debug;
use std::future::Future;
use std::io::BufRead;
use tokio::io::AsyncWriteExt;
use tokio::select;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::kv::{KvClient, KvPayload, MsgIDProvider};
use crate::message::{Body, Init, Message, MinBody, PayloadInit};

pub trait Node: Clone + Sized + Send + 'static {
    type Payload: Debug + Serialize + DeserializeOwned + Send + 'static;
    type PayloadSupplied: Debug + Send + 'static;

    fn from_init(
        init: Init,
        kv: KvClient,
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
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;

    let mut stdin = spawn_stdin();
    let mut stdout = tokio::io::stdout();

    let (tx_node, mut rx_node) = channel::<Message<Body<N::Payload>>>(1);
    let (tx_kv, mut rx_kv) = channel(1);
    let id_provider = MsgIDProvider::new();

    let (node, mut rx_node_supplied, kv) = select! {
        _ = sigint.recv() => {
            return Ok(())
        }
        _ = sigterm.recv() => {
            return Ok(())
        }
        res = handle_init::<N>(&mut stdout, id_provider, &mut stdin, tx_kv) => {
            let (node, rx_node, kv) = res?;
            (node, rx_node, kv)
        }
    };

    let handle = tokio::spawn(async move {
        let mut node_running = true;
        let mut kv_running = true;
        loop {
            select! {
                msg = rx_node.recv(), if node_running => {
                    let msg = match msg {
                        Some(msg) => msg,
                        None => {
                            node_running = false;
                            continue;
                        }
                    };
                    debug!("sending msg {msg:?}");
                    match serde_json::to_string(&msg) {
                        Ok(mut outbound) => {
                            outbound.push('\n');

                            if let Err(err) = stdout.write_all(outbound.as_bytes()).await {
                                error!("failed to write node msg to stdout: {err}");
                            }
                        }
                        Err(err) => {
                            error!(
                                "failed to serialize outbound node msg {msg:?} before writing: {err}"
                            );
                        }
                    }
                },
                msg = rx_kv.recv(), if kv_running => {
                    let msg = match msg {
                        Some(msg) => msg,
                        None => {
                            kv_running = false;
                            continue;
                        }
                    };
                    debug!("sending kv msg {msg:?}");
                    match serde_json::to_string(&msg) {
                        Ok(mut outbound) => {
                            outbound.push('\n');

                            if let Err(err) = stdout.write_all(outbound.as_bytes()).await {
                                error!("failed to write kv msg to stdout: {err}");
                            }
                        }
                        Err(err) => {
                            error!(
                                "failed to serialize outbound kv msg {msg:?} before writing: {err}"
                            );
                        }
                    }
                },
                else => {
                    break;
                }
            }
        }
    });

    let mut set = JoinSet::new();

    let mut supplied_open = true;

    loop {
        select! {
            _ = sigint.recv() => {
                break
            }
            _ = sigterm.recv() => {
                break
            }
            line = stdin.recv() => {
                let Some(line) = line else {
                    warn!("stdin channel closed");
                    break
                };
                let line = line.context("stdin recv returned error before JSON parsing")?;

                let msg: Message<&RawValue> = serde_json::from_str(line.as_str())
                    .with_context(|| format!("failed to parse inbound message envelope: {line}"))?;
                let min_body: MinBody = serde_json::from_str(msg.body.get()).with_context(|| {
                    format!("failed to parse inbound minimal body from payload: {}", msg.body.get())
                })?;

                if let Some(reply_id) = min_body.in_reply_to
                    && kv.should_process(reply_id).await?
                {
                    let (bdy, msg) = msg.extract_body();
                    let payload: Body<KvPayload> = serde_json::from_str(bdy.get()).with_context(|| {
                        format!("failed to parse kv payload from raw body: {}", bdy.get())
                    })?;
                    let (_, msg) = msg.replace_body(payload);
                    debug!("kv processing msg {msg:?}");
                    kv.process(msg).await.context("kv failed to process msg")?;
                    continue;
                }

                let (bdy, msg) = msg.extract_body();
                let payload: Body<N::Payload> = serde_json::from_str(bdy.get()).with_context(|| {
                    format!("failed to parse node payload from raw body: {}", bdy.get())
                })?;
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
    info!("about to wait for writer task");
    handle.await.context("writer task panicked")?;
    info!("about to wait for node");
    node.stop().await.context("failed to stop node")?;
    kv.stop().await;
    info!("goodbye!");

    Ok(())
}

async fn handle_init<N: Node>(
    stdout: &mut tokio::io::Stdout,
    id_provider: MsgIDProvider,
    rx_stdin: &mut Receiver<anyhow::Result<String>>,
    tx_kv: Sender<Message<Body<KvPayload>>>,
) -> Result<(N, Receiver<N::PayloadSupplied>, KvClient), anyhow::Error> {
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

    let kv = KvClient::new(id_provider.clone(), init.node_id.clone(), tx_kv);

    let (tx_supplied, rx_supplied) = channel(1);
    let node = N::from_init(init, kv.clone(), id_provider, tx_supplied)
        .await
        .context("failed to build node")?;

    let response = Message {
        src: dst,
        dst: src,
        body: Body {
            incoming_msg_id: Some(0),
            in_reply_to: id,
            payload: PayloadInit::InitOk,
        },
    };

    let mut resp = serde_json::to_string(&response).context("failed to marshal json response")?;
    resp.push('\n');
    stdout
        .write(resp.as_bytes())
        .await
        .context("failed to write to stdout")?;

    Ok((node, rx_supplied, kv))
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
