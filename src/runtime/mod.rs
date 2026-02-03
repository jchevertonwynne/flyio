mod io;
mod node;
mod routes;
mod service;

pub use node::Node;
pub use service::Service;
pub use routes::ServiceSlot;

use anyhow::Context;
use tokio::select;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc::channel;
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, fmt};

use crate::kv::{KvPayload, MsgIDProvider};
use crate::message::{Body, Message, PayloadInit};

use io::{spawn_stdin, stdout_writer_loop};
use node::{handle_init, handle_node_supplied_message, process_line};
use routes::RouteRegistry;

/// Runs the main event loop for a node.
///
/// # Errors
///
/// Returns an error if:
/// - Signal handlers fail to initialize
/// - Node initialization fails
/// - The writer task panics
/// - Node cleanup fails
pub async fn main_loop<N: Node>() -> anyhow::Result<()> {
    init_tracing();

    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut shutdown = std::pin::pin!(futures_lite::future::race(sigint.recv(), sigterm.recv()));

    let mut stdin = spawn_stdin();

    let (tx_node, rx_node) = channel::<Message<Body<N::Payload>>>(1);
    let (tx_kv, rx_kv) = channel::<Message<Body<KvPayload>>>(1);
    let (tx_init, rx_init) = channel::<Message<Body<PayloadInit>>>(1);
    let id_provider = MsgIDProvider::new();
    let routes = RouteRegistry::new();

    let writer_handle = tokio::spawn(stdout_writer_loop::<N>(rx_node, rx_kv, rx_init));

    let (node, mut rx_node_supplied, services, buffered) = select! {
        _ = &mut shutdown => {
            return Ok(())
        }
        res = handle_init::<N>(&mut stdin, id_provider, tx_kv, tx_init, routes.clone()) => {
            res?
        }
    };

    let mut set = JoinSet::new();

    for line in buffered {
        if let Err(err) = process_line(line, &services, &node, &tx_node, &routes, &mut set).await {
            error!("failed to process buffered init message: {err}");
        }
    }

    let mut supplied_open = true;

    loop {
        select! {
            _ = &mut shutdown => {
                return Ok(())
            }
            line = stdin.recv() => {
                let Some(line) = line else {
                    warn!("stdin channel closed");
                    break
                };
                let line = line.context("stdin recv returned error before JSON parsing")?;

                 if let Err(err) = process_line(line, &services, &node, &tx_node, &routes, &mut set).await {
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

                set.spawn(handle_node_supplied_message(supplied, node, tx));

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

static TRACING_INIT: std::sync::Once = std::sync::Once::new();

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
