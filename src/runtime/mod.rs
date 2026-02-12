mod io;
mod node;
mod routes;
pub mod sender;
mod service;
mod worker;

pub use node::Node;
pub use routes::ServiceSlot;
pub use sender::{ChannelSender, CollectSender};
pub use service::Service;
pub use worker::Worker;

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
use node::{handle_init, process_line, run_node_ticks};
use routes::RouteRegistry;
use tokio_util::sync::CancellationToken;

/// Runs the main event loop for a node.
///
/// # Errors
///
/// Returns an error if:
/// - Signal handlers fail to initialize
/// - Node initialization fails
/// - The writer task panics
/// - Node cleanup fails
pub async fn main_loop<N, S, W>() -> anyhow::Result<()>
where
    N: Node<S, W>,
    S: Service,
    W: Worker<N::Payload>,
{
    init_tracing();

    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut shutdown = std::pin::pin!(futures_lite::future::race(sigint.recv(), sigterm.recv()));

    let mut stdin = spawn_stdin();

    let (tx_node, rx_node) = channel::<Message<Body<N::Payload>>>(1);
    let (tx_kv, rx_kv) = channel::<Message<Body<KvPayload>>>(1);
    let (tx_init, rx_init) = channel::<Message<Body<PayloadInit>>>(1);
    let node_sender = ChannelSender::new(tx_node.clone());
    let kv_sender = ChannelSender::new(tx_kv.clone());
    let init_sender = ChannelSender::new(tx_init.clone());
    let id_provider = MsgIDProvider::new();
    let routes = RouteRegistry::new();

    let writer_handle = tokio::spawn(stdout_writer_loop::<N, S, W>(rx_node, rx_kv, rx_init));

    let (node, services, buffered): (N, S, Vec<String>) = select! {
        _ = &mut shutdown => {
            return Ok(())
        }
        res = handle_init::<N, S, W, _, _>(
            &mut stdin,
            id_provider,
            kv_sender.clone(),
            init_sender.clone(),
            routes.clone(),
        ) => {
            res?
        }
    };

    let mut set = JoinSet::new();

    let tick_cancel = CancellationToken::new();
    if let Some(worker) = node.get_worker()
        && let Some(period) = worker.tick_interval()
    {
        let tx_clone = node_sender.clone();
        let cancel = tick_cancel.clone();
        set.spawn(run_node_ticks(worker, tx_clone, cancel, period));
    }

    for line in buffered {
        let node_sender = node_sender.clone();
        if let Err(err) = process_line(line, &services, &node, node_sender, &routes, &mut set).await
        {
            error!("failed to process buffered init message: {err}");
        }
    }

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

                let node_sender = node_sender.clone();
                if let Err(err) = process_line(line, &services, &node, node_sender, &routes, &mut set).await {
                     error!("failed to process inbound message: {err}");
                }
            }
        }
    }

    drop(tx_node);
    tick_cancel.cancel();

    while let Some(result) = set.join_next().await {
        if let Err(err) = result {
            error!("node task panicked: {err}");
        }
    }

    info!("about to wait for writer task");
    writer_handle.await.context("writer task panicked")?;
    info!("about to wait for node");
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
