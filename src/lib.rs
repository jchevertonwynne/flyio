use std::fmt::Debug;
use std::future::Future;
use std::marker::Send;

use anyhow::{Context, bail};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io::BufRead;
use tokio::io::AsyncWriteExt;
use tokio::select;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::JoinSet;

pub trait Node: Clone + Sized + Send + 'static {
    type Payload: Debug + Serialize + DeserializeOwned + Send + 'static;
    type PayloadSupplied: Debug + Send + 'static;

    fn from_init(init: Init, rx: Sender<Self::PayloadSupplied>) -> anyhow::Result<Self>;
    fn handle(
        &self,
        msg: Message<Self::Payload>,
        tx: Sender<Message<Self::Payload>>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn handle_supplied(
        &self,
        msg: Self::PayloadSupplied,
        tx: Sender<Message<Self::Payload>>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn stop(&self) -> impl Future<Output = anyhow::Result<()>> + Send;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Payload> {
    #[serde(rename = "src")]
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    #[serde(rename = "body")]
    pub body: Body<Payload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub incoming_msg_id: Option<u64>,
    #[serde(rename = "in_reply_to")]
    pub in_reply_to: Option<u64>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PayloadInit {
    Init(Init),
    InitOk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init {
    #[serde(rename = "node_id")]
    pub node_id: String,
    #[serde(rename = "node_ids")]
    pub node_ids: Vec<String>,
}

pub async fn main_loop<N: Node>() -> anyhow::Result<()> {
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;

    let mut stdin = spawn_stdin();
    let mut stdout = tokio::io::stdout();

    let (node, mut rx_node) = select! {
        _ = sigint.recv() => {
            return Ok(())
        }
        _ = sigterm.recv() => {
            return Ok(())
        }
        res = handle_init::<N>(&mut stdout, &mut stdin) => {
            let (node, rx_node) = res?;
            (node, rx_node)
        }
    };

    let (tx, mut rx) = channel::<Message<N::Payload>>(1);

    let handle = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            eprintln!("sending msg {msg:?}");
            let mut msg = serde_json::to_string(&msg).unwrap();
            msg.push('\n');

            if let Err(e) = stdout.write_all(msg.as_bytes()).await {
                eprintln!("failed to write msg to stdout: {e}");
            }
        }
    });

    let mut set = JoinSet::new();

    loop {
        select! {
            _ = sigint.recv() => {
                break
            }
            _ = sigterm.recv() => {
                break
            }
            line = stdin.recv() => {
                let Some(line) = line else { break };
                let line = line.context("failed to read line")?;

                let msg: Message<N::Payload> =
                    serde_json::from_str(&line).context("failed to deserialize msg")?;

                let tx = tx.clone();
                let node = node.clone();

                set.spawn(async move {
                    eprintln!("handling msg {msg:?}");
                    let resp = node.handle(msg, tx).await;
                    if let Err(err) = resp {
                        eprintln!("node handle failed: {err}");
                    }
                });
            }
            supplied = rx_node.recv() => {
                let Some(supplied) = supplied else { break };

                let tx = tx.clone();
                let node = node.clone();

                set.spawn(async move {
                    eprintln!("handling supplied msg {supplied:?}");
                    let resp = node.handle_supplied(supplied, tx).await;
                    if let Err(err) = resp {
                        eprintln!("node handle failed: {err}");
                    }
                });

            }
        }
    }

    drop(tx);
    eprintln!("about to wait for writer task");
    handle.await.context("writer task panicked")?;
    eprintln!("about to wait for node");
    node.stop().await.context("failed to stop node")?;
    eprintln!("goodbye!");

    Ok(())
}

async fn handle_init<N: Node>(
    stdout: &mut tokio::io::Stdout,
    stdin: &mut Receiver<anyhow::Result<String>>,
) -> Result<(N, Receiver<N::PayloadSupplied>), anyhow::Error> {
    let line = stdin
        .recv()
        .await
        .context("failed to read from stdin")?
        .context("no line was present for init msg")?;
    let init_msg: Message<PayloadInit> =
        serde_json::from_str(&line).context("failed to parse init msg")?;
    eprintln!("recieved init msg: {init_msg:?}");

    let Message { src, dst, body } = init_msg;
    let Body {
        incoming_msg_id: id,
        in_reply_to: _,
        payload,
    } = body;
    let PayloadInit::Init(init) = payload else {
        bail!("should not receive an InitOk msg first")
    };

    let (tx_supplied, rx_supplied) = channel(1);
    let node = N::from_init(init, tx_supplied).context("failed to build node")?;

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

    Ok((node, rx_supplied))
}

fn spawn_stdin() -> Receiver<anyhow::Result<String>> {
    let (tx, rx) = channel(1);

    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            if tx
                .blocking_send(line.context("failed to read line"))
                .is_err()
            {
                break;
            }
        }
    });

    rx
}
