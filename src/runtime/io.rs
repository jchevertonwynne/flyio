use anyhow::Context;
use serde::Serialize;
use std::fmt::Debug;
use std::io::BufRead;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::Receiver;
use tracing::{debug, error, warn};

use crate::kv::KvPayload;
use crate::message::{Body, Message, PayloadInit};

use super::node::Node;

pub(crate) async fn stdout_writer_loop<N: Node>(
    mut rx_node: Receiver<Message<Body<N::Payload>>>,
    mut rx_kv: Receiver<Message<Body<KvPayload>>>,
    mut rx_init: Receiver<Message<Body<PayloadInit>>>,
) {
    let mut stdout = tokio::io::stdout();
    let mut node_running = true;
    let mut kv_running = true;
    let mut init_running = true;

    loop {
        tokio::select! {
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
}

async fn handle_output<T: Serialize + Debug>(
    msg: Option<T>,
    running: &mut bool,
    ctx: &str,
    stdout: &mut tokio::io::Stdout,
) {
    let Some(msg) = msg else {
        *running = false;
        return;
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
