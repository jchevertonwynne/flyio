use std::{
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, bail};
use flyio::{Body, Init, Message, Node, main_loop};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

#[derive(Clone)]
struct EchoNode {
    inner: Arc<EchoNodeInner>,
}

struct EchoNodeInner {
    id: AtomicU64,
}

impl Deref for EchoNode {
    type Target = EchoNodeInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

impl Node for EchoNode {
    type Payload = EchoPayload;
    type PayloadSupplied = ();

    fn from_init(_init: Init, _tx: Sender<Self::PayloadSupplied>) -> anyhow::Result<Self> {
        Ok(EchoNode {
            inner: Arc::new(EchoNodeInner {
                id: AtomicU64::new(1),
            }),
        })
    }

    async fn handle(
        &self,
        msg: Message<Self::Payload>,
        tx: Sender<Message<Self::Payload>>,
    ) -> anyhow::Result<()> {
        let Message { src, dst, body } = msg;
        let Body {
            incoming_msg_id: id,
            in_reply_to: _,
            payload,
        } = body;

        match payload {
            EchoPayload::Echo { echo } => {
                let resp_msg_id = self.id.fetch_add(1, Ordering::SeqCst);
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
        _tx: Sender<Message<Self::Payload>>,
    ) -> anyhow::Result<()> {
        bail!("this should never receive supplied events")
    }

    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop::<EchoNode>().await
}
