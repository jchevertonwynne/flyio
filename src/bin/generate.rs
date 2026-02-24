use std::{ops::Deref, sync::Arc};

use anyhow::{Context, bail};
use async_trait::async_trait;
use flyio::{Body, Init, Message, MessageSender, MsgIDProvider, Node, main_loop};
use serde::{Deserialize, Serialize};
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
impl Node<(), ()> for GenerateNode {
    type Payload = GeneratePayload;

    async fn from_init(
        init: Init,
        _service: (),
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

    async fn handle<T>(&self, msg: Message<Body<Self::Payload>>, tx: T) -> anyhow::Result<()>
    where
        T: MessageSender<Self::Payload>,
    {
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

                tx.send(response)
                    .await
                    .context("failed to send generate response")?;
            }
            GeneratePayload::GenerateOk { .. } => bail!("i should not receive this"),
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop::<GenerateNode, (), ()>()
        .await
        .inspect_err(|err| error!("failed to run main: {err}"))
}

#[cfg(test)]
mod tests {
    use flyio::CollectSender;

    use super::*;

    fn mk_node() -> GenerateNode {
        GenerateNode {
            inner: Arc::new(GenerateNodeInner {
                node_id: "n2".into(),
                id_provider: MsgIDProvider::new(),
            }),
        }
    }

    fn mk_message(payload: GeneratePayload) -> Message<Body<GeneratePayload>> {
        Message {
            src: "client".into(),
            dst: "n2".into(),
            body: Body {
                incoming_msg_id: Some(5),
                in_reply_to: None,
                payload,
            },
        }
    }

    #[tokio::test]
    async fn returns_scoped_id_for_generate() {
        let node = mk_node();
        let handle = CollectSender::default();

        node.handle(mk_message(GeneratePayload::Generate), handle.clone())
            .await
            .expect("handle should succeed");

        let mut responses = handle.drain().await;
        let response = responses.pop().expect("response missing");
        #[allow(clippy::match_wildcard_for_single_variants)]
        match response.body.payload {
            GeneratePayload::GenerateOk { id } => {
                assert!(id.starts_with("n2-"));
                assert_eq!(response.body.in_reply_to, Some(5));
                assert!(response.body.incoming_msg_id.is_some());
            }
            payload => panic!("unexpected payload: {payload:?}"),
        }
    }

    #[tokio::test]
    async fn rejects_generate_ok_payload() {
        let node = mk_node();
        let handle = CollectSender::default();

        let err = node
            .handle(
                mk_message(GeneratePayload::GenerateOk {
                    id: "n2-123".into(),
                }),
                handle,
            )
            .await
            .expect_err("should fail for GenerateOk request");

        assert!(err.to_string().contains("should not receive"));
    }
}
