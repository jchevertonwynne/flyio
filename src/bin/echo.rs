use anyhow::{Context, bail};
use flyio::{Body, Init, KvClient, Message, MsgIDProvider, Node, main_loop};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use tokio::sync::mpsc::Sender;
use tracing::error;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Clone)]
struct EchoNode {
    id_provider: MsgIDProvider,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

impl Node for EchoNode {
    type Payload = EchoPayload;
    type PayloadSupplied = Infallible;

    async fn from_init(
        _init: Init,
        _kv: KvClient,
        id_provider: MsgIDProvider,
        _tx: Sender<Self::PayloadSupplied>,
    ) -> anyhow::Result<Self> {
        Ok(EchoNode { id_provider })
    }

    async fn handle(
        &self,
        msg: Message<Body<Self::Payload>>,
        tx: Sender<Message<Body<Self::Payload>>>,
    ) -> anyhow::Result<()> {
        let Message { src, dst, body } = msg;
        let Body {
            incoming_msg_id: id,
            in_reply_to: _,
            payload,
        } = body;

        match payload {
            EchoPayload::Echo { echo } => {
                let resp_msg_id = self.id_provider.id();
                let response = Message {
                    src: dst,
                    dst: src,
                    body: Body {
                        incoming_msg_id: Some(resp_msg_id),
                        in_reply_to: id,
                        payload: EchoPayload::EchoOk { echo },
                    },
                };

                tx.send(response).await.context("channel closed")?;
            }
            EchoPayload::EchoOk { echo: _ } => bail!("i should not receive this"),
        }

        Ok(())
    }

    async fn handle_supplied(
        &self,
        _msg: Self::PayloadSupplied,
        _tx: Sender<Message<Body<Self::Payload>>>,
    ) -> anyhow::Result<()> {
        bail!("this should never receive supplied events")
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    main_loop::<EchoNode>()
        .await
        .inspect_err(|err| error!("failed to run main: {err}"))
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    let _ = fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init();
}
