use anyhow::{Context, bail};
use async_trait::async_trait;
use flyio::{
    Body, Init, KvClientTimeoutExt, Message, MessageSender, MsgIDProvider, Node,
    SeqKvClient, Worker, main_loop,
};
use serde::{Deserialize, Serialize};
use std::{
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tracing::{debug, error, warn};

const COUNTER_KEY: &str = "shared_counter";
const KV_TIMEOUT: Duration = Duration::from_millis(250);

#[derive(Clone)]
struct GrowNode {
    inner: Arc<GrowNodeInner>,
}

struct GrowNodeInner {
    node_id: String,
    id_provider: MsgIDProvider,
    propogate: AtomicU64,
    kv: SeqKvClient,
}

impl GrowNodeInner {
    async fn flush_pending(&self) -> anyhow::Result<()> {
        let pending = self.propogate.swap(0, Ordering::SeqCst);
        if pending == 0 {
            return Ok(());
        }

        let mut sleep_duration = Duration::from_millis(1);
        loop {
            let value: u64 = self.kv.read_with_timeout(COUNTER_KEY, KV_TIMEOUT).await?;
            let new_value = value
                .checked_add(pending)
                .context("shared counter would overflow u64")?;

            if self
                .kv
                .compare_and_swap_with_timeout(COUNTER_KEY, value, new_value, false, KV_TIMEOUT)
                .await?
            {
                debug!(value, new_value, pending, "flushed pending delta");
                break;
            }

            debug!(value, new_value, pending, "flush contention; retrying");
            tokio::time::sleep(sleep_duration).await;
            sleep_duration = sleep_duration
                .saturating_mul(2)
                .min(Duration::from_millis(100));
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

impl Add {
    async fn handle<T>(
        self,
        msg: Message<Body<()>>,
        node: &GrowNode,
        tx: T,
    ) -> anyhow::Result<()>
    where
        T: MessageSender<GrowNodePayload>,
    {
        let Self { delta: message } = self;
        let Message {
            src,
            dst,
            body:
                Body {
                    incoming_msg_id,
                    in_reply_to: _,
                    payload: (),
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

        tx.send(response).await.context("failed to send add ack")?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct AddOk;

impl AddOk {
    #[allow(clippy::unused_async)]
    async fn handle<T>(
        self,
        _msg: Message<Body<()>>,
        _node: &GrowNode,
        _tx: T,
    ) -> anyhow::Result<()>
    where
        T: MessageSender<GrowNodePayload>,
    {
        bail!("unexpected AddOk message")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Read;

impl Read {
    async fn handle<T>(
        self,
        msg: Message<Body<()>>,
        node: &GrowNode,
        tx: T,
    ) -> anyhow::Result<()>
    where
        T: MessageSender<GrowNodePayload>,
    {
        let Message {
            src,
            dst,
            body:
                Body {
                    incoming_msg_id,
                    in_reply_to: _,
                    payload: (),
                },
        } = msg;

        node.flush_pending().await?;

        let sync_key = format!("sync-{}", node.node_id);
        let sync_val = node.id_provider.id();

        node.kv
            .write_with_timeout(sync_key.as_str(), sync_val, KV_TIMEOUT)
            .await
            .context("failed to write sync key")?;

        let value = match node
            .kv
            .read_with_timeout::<u64>(COUNTER_KEY, KV_TIMEOUT)
            .await
        {
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

        tx.send(response)
            .await
            .context("failed to send read response")?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct ReadOk {
    value: u64,
}

impl ReadOk {
    #[allow(clippy::unused_async)]
    async fn handle<T>(
        self,
        _msg: Message<Body<()>>,
        _node: &GrowNode,
        _tx: T,
    ) -> anyhow::Result<()>
    where
        T: MessageSender<GrowNodePayload>,
    {
        bail!("unexpected ReadOk message")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct Error {
    code: u64,
    text: String,
}

impl Error {
    #[allow(clippy::unused_async)]
    async fn handle<T>(
        self,
        _msg: Message<Body<()>>,
        _node: &GrowNode,
        _tx: T,
    ) -> anyhow::Result<()>
    where
        T: MessageSender<GrowNodePayload>,
    {
        let Self { code, text } = self;
        warn!("error payload recieved: code = {code} text = {text}");

        Ok(())
    }
}

impl GrowNodePayload {
    async fn dispatch<T>(
        self,
        msg: Message<Body<()>>,
        node: &GrowNode,
        tx: T,
    ) -> anyhow::Result<()>
    where
        T: MessageSender<GrowNodePayload>,
    {
        match self {
            GrowNodePayload::Add(payload) => payload.handle(msg, node, tx).await,
            GrowNodePayload::Read(payload) => payload.handle(msg, node, tx).await,
            GrowNodePayload::Error(payload) => payload.handle(msg, node, tx).await,
            GrowNodePayload::AddOk(payload) => payload.handle(msg, node, tx).await,
            GrowNodePayload::ReadOk(payload) => payload.handle(msg, node, tx).await,
        }
    }
}

#[async_trait]
impl Worker<GrowNodePayload> for GrowNode {
    fn tick_interval(&self) -> Option<Duration> {
        Some(Duration::from_millis(100))
    }

    async fn handle_tick<T: MessageSender<GrowNodePayload>>(&self, _tx: T) -> anyhow::Result<()> {
        if self.propogate.load(Ordering::SeqCst) > 0 {
            self.flush_pending().await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Node<SeqKvClient, Self> for GrowNode {
    type Payload = GrowNodePayload;

    async fn from_init(
        init: Init,
        kv: SeqKvClient,
        id_provider: MsgIDProvider,
    ) -> anyhow::Result<Self> {
        loop {
            match kv
                .compare_and_swap_with_timeout(COUNTER_KEY, 0, 0, true, KV_TIMEOUT)
                .await
            {
                Ok(_) => break,
                Err(flyio::KvError::KeyDoesNotExist) => {}
                Err(e) => return Err(e).context("failed to init counter"),
            }
        }
        Ok(GrowNode {
            inner: Arc::new(GrowNodeInner {
                node_id: init.node_id,
                id_provider,
                propogate: AtomicU64::new(0),
                kv,
            }),
        })
    }

    async fn handle<T>(
        &self,
        msg: Message<Body<Self::Payload>>,
        tx: T,
    ) -> anyhow::Result<()>
    where
        T: MessageSender<Self::Payload>,
    {
        let (payload, message) = msg.replace_payload(());
        payload.dispatch(message, self, tx).await
    }

    fn get_worker(&self) -> Option<Self> {
        Some(self.clone())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop::<GrowNode, SeqKvClient, GrowNode>()
        .await
        .inspect_err(|err| error!("failed to run main: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flyio::{ChannelSender, KvPayload};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn mk_node() -> GrowNode {
        let (tx_payload, mut rx_payload) =
            tokio::sync::mpsc::channel::<Message<Body<KvPayload>>>(8);
        tokio::spawn(async move { while rx_payload.recv().await.is_some() {} });
        let tx_payload = ChannelSender::new(tx_payload);

        GrowNode {
            inner: Arc::new(GrowNodeInner {
                node_id: "n1".into(),
                id_provider: MsgIDProvider::new(),
                propogate: AtomicU64::new(0),
                kv: SeqKvClient::new(MsgIDProvider::new(), "n1".into(), tx_payload),
            }),
        }
    }

    fn mk_message(incoming_msg_id: Option<u64>) -> Message<Body<()>> {
        Message {
            src: "client".into(),
            dst: "n1".into(),
            body: Body {
                incoming_msg_id,
                in_reply_to: None,
                payload: (),
            },
        }
    }

    #[tokio::test]
    async fn add_handler_accumulates_and_responds() {
        let node = mk_node();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let handle = ChannelSender::new(tx);

        Add { delta: 7 }
            .handle(mk_message(Some(99)), &node, handle)
            .await
            .expect("add handler should succeed");

        assert_eq!(node.propogate.load(Ordering::SeqCst), 7);

        let response = rx.recv().await.expect("missing response");
        match response.body.payload {
            GrowNodePayload::AddOk(_) => {
                assert_eq!(response.body.in_reply_to, Some(99));
                assert!(response.body.incoming_msg_id.is_some());
            }
            payload => panic!("unexpected payload: {payload:?}"),
        }

        node.kv.stop().await;
    }

    #[tokio::test]
    async fn flush_pending_is_noop_when_no_delta() {
        let node = mk_node();

        node.flush_pending()
            .await
            .expect("flush should return early when no pending delta");

        assert_eq!(node.propogate.load(Ordering::SeqCst), 0);
        node.kv.stop().await;
    }

    #[tokio::test]
    async fn add_ok_handler_is_unexpected() {
        let node = mk_node();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let handle = ChannelSender::new(tx);

        let err = AddOk
            .handle(mk_message(Some(5)), &node, handle)
            .await
            .expect_err("AddOk handler should error on inbound message");

        assert!(err.to_string().contains("unexpected AddOk"));
        node.kv.stop().await;
    }
}
