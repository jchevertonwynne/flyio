use anyhow::{Context, bail};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;
use std::fmt::Debug;
use std::future::Future;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::task::JoinSet;
use tracing::{debug, error, warn};

use crate::kv::{KvPayload, MsgIDProvider};
use crate::message::{Body, Init, Message, MinBody, PayloadInit};

use super::routes::{RouteRegistry, ServiceSlot};
use super::service::Service;

pub trait Node: Clone + Sized + Send + 'static {
    type Payload: Debug + Serialize + DeserializeOwned + Send + 'static;
    type PayloadSupplied: Debug + Send + 'static;
    type Service: Service;

    fn from_init(
        init: Init,
        service: Self::Service,
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

pub(crate) async fn process_line<N: Node>(
    line: String,
    services: &N::Service,
    node: &N,
    tx_node: &Sender<Message<Body<N::Payload>>>,
    routes: &RouteRegistry,
    set: &mut JoinSet<()>,
) -> anyhow::Result<()> {
    let (msg, min_body) = parse_envelope(line.as_str())?;
    let Some(msg) = handle_service_message(msg, &min_body, services, routes).await? else {
        return Ok(());
    };

    let (bdy, msg) = msg.extract_body();
    let payload: Body<N::Payload> = serde_json::from_str(bdy.get())
        .with_context(|| format!("failed to parse node payload from raw body: {}", bdy.get()))?;
    let ((), msg) = msg.replace_body(payload);

    let tx = tx_node.clone();
    let node = node.clone();

    set.spawn(handle_node_message(msg, node, tx));

    Ok(())
}

pub(crate) async fn handle_node_message<N: Node>(
    msg: Message<Body<N::Payload>>,
    node: N,
    tx: Sender<Message<Body<N::Payload>>>,
) {
    debug!("handling msg {msg:?}");
    let resp = node.handle(msg, tx).await;
    if let Err(err) = resp {
        error!("node handle failed: {err}");
    }
}

pub(crate) async fn handle_node_supplied_message<N: Node>(
    supplied: N::PayloadSupplied,
    node: N,
    tx: Sender<Message<Body<N::Payload>>>,
) {
    debug!("handling supplied msg {supplied:?}");
    let resp = node.handle_supplied(supplied, tx).await;
    if let Err(err) = resp {
        error!("node handle supplied failed: {err}");
    }
}

pub(crate) async fn handle_init<N: Node>(
    rx_stdin: &mut Receiver<anyhow::Result<String>>,
    id_provider: MsgIDProvider,
    tx_kv: Sender<Message<Body<KvPayload>>>,
    tx_init: Sender<Message<Body<PayloadInit>>>,
    routes: RouteRegistry,
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

    let services = N::Service::init(
        id_provider.clone(),
        init.node_id.clone(),
        tx_kv,
        routes.clone(),
        ServiceSlot::root(),
    );

    let (tx_supplied, rx_supplied) = channel(1);

    let services_init = services.clone();
    let mut init_node_fut = std::pin::pin!(async move {
        N::from_init(init, services_init, id_provider, tx_supplied)
            .await
            .context("failed to build node")
    });

    let services_clone = services.clone();
    let mut buffered_lines = Vec::new();

    let node = loop {
        select! {
            node = &mut init_node_fut => {
                break node?;
            }
            line = rx_stdin.recv() => {
                let Some(line) = line else {
                    bail!("stdin closed during init");
                };
                let line = line.context("stdin recv error during init")?;

                let (msg, min_body) = parse_envelope(&line)?;
                if handle_service_message(msg, &min_body, &services_clone, &routes)
                    .await?
                    .is_some()
                {
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

pub(crate) async fn handle_service_message<'a, S: Service>(
    msg: Message<&'a RawValue>,
    min_body: &MinBody,
    services: &S,
    routes: &RouteRegistry,
) -> anyhow::Result<Option<Message<&'a RawValue>>> {
    if let Some(reply_id) = min_body.in_reply_to
        && let Some(slot) = routes.take(reply_id)
    {
        let (bdy, msg) = msg.extract_body();
        let payload: Body<KvPayload> = serde_json::from_str(bdy.get())
            .with_context(|| format!("failed to parse kv payload from raw body: {}", bdy.get()))?;
        let ((), msg) = msg.replace_body(payload);
        debug!("kv routing msg {msg:?} via slot {slot:?}");
        services
            .dispatch(slot, msg)
            .await
            .context("kv failed to process msg")?;
        return Ok(None);
    }

    Ok(Some(msg))
}

fn parse_envelope<'a>(line: &'a str) -> anyhow::Result<(Message<&'a RawValue>, MinBody)> {
    let msg: Message<&'a RawValue> = serde_json::from_str(line)
        .with_context(|| format!("failed to parse inbound message envelope: {line}"))?;
    let min_body: MinBody = serde_json::from_str(msg.body.get()).with_context(|| {
        format!(
            "failed to parse inbound minimal body from payload: {}",
            msg.body.get()
        )
    })?;
    Ok((msg, min_body))
}
