use std::{ops::Deref, sync::Arc};

use anyhow::{Context, bail};
use async_trait::async_trait;
use flyio::{Body, Init, Message, MsgIDProvider, SimpleNode, main_loop};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use tracing::error;

#[derive(Clone)]
struct GenerateNode {
    inner: Arc<GenerateNodeInner>,
}

struct GenerateNodeInner {
    node_id: String,
    id_provider: MsgIDProvider,
}

impl Deref for GenerateNode {
    type Target = GenerateNodeInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum GeneratePayload {
    Generate,
    GenerateOk { id: String },
}

#[async_trait]
impl SimpleNode for GenerateNode {
    type Payload = GeneratePayload;

    async fn from_init_simple(
        init: Init,
        id_provider: MsgIDProvider,
    ) -> anyhow::Result<Self> {
        let Init {
            node_id,
            node_ids: _,
        } = init;

        Ok(GenerateNode {
            inner: Arc::new(GenerateNodeInner {
                node_id,
                id_provider,
            }),
        })
    }

    async fn handle_simple(
        &self,
        msg: Message<Body<Self::Payload>>,
        tx: Sender<Message<Body<Self::Payload>>>,
    ) -> anyhow::Result<()> {
        let Message {
            src,
            dst,
            body:
                Body {
                    incoming_msg_id: id,
                    in_reply_to: _,
                    payload,
                },
        } = msg;

        match payload {
            GeneratePayload::Generate => {
                let resp_msg_id = self.id_provider.id();

                let response = Message {
                    src: dst,
                    dst: src,
                    body: Body {
                        incoming_msg_id: Some(resp_msg_id),
                        in_reply_to: id,
                        payload: GeneratePayload::GenerateOk {
                            id: format!("{}-{}", self.node_id, resp_msg_id),
                        },
                    },
                };

                tx.send(response).await.context("channel closed")?;
            }
            GeneratePayload::GenerateOk { .. } => bail!("i should not receive this"),
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop::<GenerateNode>()
        .await
        .inspect_err(|err| error!("failed to run main: {err}"))
}
