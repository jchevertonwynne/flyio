use anyhow::{Context, bail};
use async_trait::async_trait;
use flyio::{Body, Init, Message, MessageSender, MsgIDProvider, Node, main_loop};
use serde::{Deserialize, Serialize};
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
impl Node<(), ()> for EchoNode {
    type Payload = EchoPayload;

    async fn from_init(
        _init: Init,
        _service: (),
        id_provider: MsgIDProvider,
    ) -> anyhow::Result<Self> {
        Ok(EchoNode { id_provider })
    }

    async fn handle<T>(&self, msg: Message<Body<Self::Payload>>, tx: T) -> anyhow::Result<()>
    where
        T: MessageSender<Self::Payload>,
    {
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

                tx.send(response)
                    .await
                    .context("failed to send echo response")?;
            }
            EchoPayload::EchoOk { echo: _ } => bail!("i should not receive this"),
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop::<EchoNode, (), ()>()
        .await
        .inspect_err(|err| error!("failed to run main: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flyio::{CollectSender, MsgIDProvider};

    fn mk_node() -> EchoNode {
        EchoNode {
            id_provider: MsgIDProvider::new(),
        }
    }

    fn mk_message(payload: EchoPayload) -> Message<Body<EchoPayload>> {
        Message {
            src: "n1".into(),
            dst: "n2".into(),
            body: Body {
                incoming_msg_id: Some(42),
                in_reply_to: None,
                payload,
            },
        }
    }

    #[tokio::test]
    async fn echoes_back_with_same_payload() {
        let node = mk_node();
        let handle = CollectSender::default();

        node.handle(
            mk_message(EchoPayload::Echo { echo: "hi".into() }),
            handle.clone(),
        )
        .await
        .expect("handle should succeed");

        let mut responses = handle.drain().await;
        let response = responses.pop().expect("response missing");
        #[allow(clippy::match_wildcard_for_single_variants)]
        match response.body.payload {
            EchoPayload::EchoOk { echo } => {
                assert_eq!(echo, "hi");
                assert_eq!(response.body.in_reply_to, Some(42));
                assert!(response.body.incoming_msg_id.is_some());
            }
            payload => panic!("unexpected payload: {payload:?}"),
        }
    }

    #[tokio::test]
    async fn rejects_echo_ok_payload() {
        let node = mk_node();
        let handle = CollectSender::default();

        let err = node
            .handle(
                mk_message(EchoPayload::EchoOk {
                    echo: "oops".into(),
                }),
                handle,
            )
            .await
            .expect_err("should fail when receiving EchoOk");

        assert!(err.to_string().contains("should not receive"));
    }
}
