use anyhow::{Context, bail};
use async_trait::async_trait;
use flyio::{Body, Init, Message, MsgIDProvider, SimpleNode, main_loop};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use tracing::error;

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

#[async_trait]
impl SimpleNode for EchoNode {
    type Payload = EchoPayload;

    async fn from_init_simple(
        _init: Init,
        id_provider: MsgIDProvider,
    ) -> anyhow::Result<Self> {
        Ok(EchoNode {
            id_provider,
        })
    }

    async fn handle_simple(
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop::<EchoNode>()
        .await
        .inspect_err(|err| error!("failed to run main: {err}"))
}
