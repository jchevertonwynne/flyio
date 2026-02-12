use anyhow::{Context, bail};
use async_trait::async_trait;
use flyio::{Body, Init, Message, MessageSender, MsgIDProvider, Node, main_loop};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ops::Deref, sync::Arc};
use tokio::sync::Mutex;
use tracing::{error, warn};

#[derive(Clone)]
struct TxnNode {
    inner: Arc<TxnNodeInner>,
}

struct TxnNodeInner {
    id_provider: MsgIDProvider,
    state: Mutex<HashMap<u64, u64>>,
}

impl Deref for TxnNode {
    type Target = TxnNodeInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TxnNodePayload {
    Txn(Txn),
    TxnOk(TxnOk),
    Error(Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
struct Txn {
    txn: Vec<Op>,
}

#[derive(Debug, Clone)]
enum Op {
    Read(u64),
    Write(u64, u64),
}

impl Serialize for Op {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_tuple(3)?;
        match self {
            Op::Read(key) => {
                state.serialize_element("r")?;
                state.serialize_element(key)?;
                state.serialize_element(&None::<u64>)?;
            }
            Op::Write(key, val) => {
                state.serialize_element("w")?;
                state.serialize_element(key)?;
                state.serialize_element(&Some(val))?;
            }
        }
        state.end()
    }
}

impl<'de> Deserialize<'de> for Op {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (rw, key, val) = <(String, u64, Option<u64>)>::deserialize(deserializer)?;
        match rw.as_str() {
            "r" => {
                if val.is_some() {
                    return Err(serde::de::Error::custom("read op should not have a value"));
                }
                Ok(Op::Read(key))
            }
            "w" => {
                let v = val.ok_or_else(|| serde::de::Error::custom("write op missing value"))?;
                Ok(Op::Write(key, v))
            }
            _ => Err(serde::de::Error::custom(format!("invalid op type: {rw}"))),
        }
    }
}

#[derive(Debug, Clone)]
enum OpResult {
    Read(u64, Option<u64>),
    Write(u64, u64),
}

impl Serialize for OpResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_tuple(3)?;
        match self {
            OpResult::Read(key, val) => {
                state.serialize_element("r")?;
                state.serialize_element(key)?;
                state.serialize_element(val)?;
            }
            OpResult::Write(key, val) => {
                state.serialize_element("w")?;
                state.serialize_element(key)?;
                state.serialize_element(&Some(val))?;
            }
        }
        state.end()
    }
}

impl<'de> Deserialize<'de> for OpResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (rw, key, val) = <(String, u64, Option<u64>)>::deserialize(deserializer)?;
        match rw.as_str() {
            "r" => Ok(OpResult::Read(key, val)),
            "w" => {
                let value =
                    val.ok_or_else(|| serde::de::Error::custom("write op missing value"))?;
                Ok(OpResult::Write(key, value))
            }
            _ => Err(serde::de::Error::custom(format!("invalid op type: {rw}"))),
        }
    }
}

impl Txn {
    async fn handle<T: MessageSender<TxnNodePayload>>(
        self,
        msg: Message<Body<()>>,
        node: &TxnNode,
        tx: T,
    ) -> anyhow::Result<()> {
        let Self { txn } = self;
        let Message { src, dst, body } = msg;

        let mut state = node.state.lock().await;

        let mut res = vec![];

        for op in txn {
            match op {
                Op::Read(key) => {
                    let value = state.get(&key).copied();
                    res.push(OpResult::Read(key, value));
                }
                Op::Write(key, value) => {
                    state.insert(key, value);
                    res.push(OpResult::Write(key, value));
                }
            }
        }

        let resp = Message {
            src: dst,
            dst: src,
            body: Body {
                incoming_msg_id: Some(node.id_provider.id()),
                in_reply_to: body.incoming_msg_id,
                payload: TxnNodePayload::TxnOk(TxnOk { txn: res }),
            },
        };

        tx.send(resp).await.context("failed to send txn response")?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
struct TxnOk {
    txn: Vec<OpResult>,
}

impl TxnOk {
    #[allow(clippy::unused_async)]
    async fn handle<T: MessageSender<TxnNodePayload>>(
        self,
        _msg: Message<Body<()>>,
        _node: &TxnNode,
        _tx: T,
    ) -> anyhow::Result<()> {
        bail!("unexpected SendOk message")
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
    async fn handle<T: MessageSender<TxnNodePayload>>(
        self,
        _msg: Message<Body<()>>,
        _node: &TxnNode,
        _tx: T,
    ) -> anyhow::Result<()> {
        let Self { code, text } = self;
        warn!("error payload recieved: code = {code} text = {text}");

        Ok(())
    }
}

impl TxnNodePayload {
    async fn dispatch<T: MessageSender<TxnNodePayload>>(
        self,
        msg: Message<Body<()>>,
        node: &TxnNode,
        tx: T,
    ) -> anyhow::Result<()> {
        use TxnNodePayload::{Error, Txn, TxnOk};
        match self {
            Txn(payload) => payload.handle(msg, node, tx).await,
            TxnOk(payload) => payload.handle(msg, node, tx).await,
            Error(payload) => payload.handle(msg, node, tx).await,
        }
    }
}

#[async_trait]
impl Node<(), ()> for TxnNode {
    type Payload = TxnNodePayload;

    async fn from_init(_init: Init, _: (), id_provider: MsgIDProvider) -> anyhow::Result<Self> {
        Ok(TxnNode {
            inner: Arc::new(TxnNodeInner {
                id_provider,
                state: Mutex::new(HashMap::new()),
            }),
        })
    }

    async fn handle<T: MessageSender<Self::Payload>>(
        &self,
        msg: Message<Body<Self::Payload>>,
        tx: T,
    ) -> anyhow::Result<()> {
        let (payload, message) = msg.replace_payload(());
        payload.dispatch(message, self, tx).await
    }

    fn get_worker(&self) -> Option<()> {
        None
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop::<TxnNode, (), ()>()
        .await
        .inspect_err(|err| error!("failed to run main: {err}"))
}
