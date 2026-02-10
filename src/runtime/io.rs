use anyhow::Context;
use futures::stream::{self, StreamExt};
use serde::Serialize;
use std::fmt::Debug;
use std::io::BufRead;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::Receiver;
use tracing::{debug, error, warn};

use crate::kv::KvPayload;
use crate::message::{Body, Message, PayloadInit};

use super::node::Node;

enum OutputMessage<P> {
    Node(Message<Body<P>>),
    Kv(Message<Body<KvPayload>>),
    Init(Message<Body<PayloadInit>>),
}

pub(crate) async fn stdout_writer_loop<N: Node<S, W>, S, W>(
    rx_node: Receiver<Message<Body<N::Payload>>>,
    rx_kv: Receiver<Message<Body<KvPayload>>>,
    rx_init: Receiver<Message<Body<PayloadInit>>>,
) {
    let mut stdout = tokio::io::stdout();

    let s_node = stream::unfold(rx_node, |mut rx| async move {
        rx.recv().await.map(|msg| (OutputMessage::Node(msg), rx))
    });
    let s_kv = stream::unfold(rx_kv, |mut rx| async move {
        rx.recv()
            .await
            .map(|msg| (OutputMessage::<N::Payload>::Kv(msg), rx))
    });
    let s_init = stream::unfold(rx_init, |mut rx| async move {
        rx.recv()
            .await
            .map(|msg| (OutputMessage::<N::Payload>::Init(msg), rx))
    });

    let mut stream = std::pin::pin!(stream::select(stream::select(s_node, s_kv), s_init));

    while let Some(msg) = stream.next().await {
        match msg {
            OutputMessage::Node(msg) => write_message(msg, "node", &mut stdout).await,
            OutputMessage::Kv(msg) => write_message(msg, "kv", &mut stdout).await,
            OutputMessage::Init(msg) => write_message(msg, "init", &mut stdout).await,
        }
    }
}

async fn write_message<T: Serialize + Debug>(msg: T, ctx: &str, stdout: &mut tokio::io::Stdout) {
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

pub(crate) fn spawn_stdin() -> Receiver<anyhow::Result<String>> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);

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
