use anyhow::Context;
use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::{Sender, channel};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, warn};

use crate::message::{Body, Message};

use super::messages::{
    KvMsg, KvMsgCmpAndSwp, KvMsgCmpAndSwpResponse, KvMsgErrorResponse, KvMsgRead,
    KvMsgReadResponse, KvMsgWrite, KvMsgWriteResponse, TsoMsg, TsoMsgTs, TsoMsgTsOk,
};
use super::payload::{Cas, HandleKvPayload, HandleTsoPayload, KvPayload, Read, Ts, Write};
use super::types::{
    ERROR_CODE_KEY_NOT_EXIST, ERROR_CODE_PRECONDITION_FAILED, KvError, LinStore, LwwStore,
    MsgIDProvider, SeqStore, StoreName,
};

/// Helper function to deliver responses to waiting channels
fn deliver_response<T>(
    waiters: &mut HashMap<u64, oneshot::Sender<T>>,
    msg_id: u64,
    result: T,
    operation: &str,
) {
    if let Some(tx) = waiters.remove(&msg_id) {
        if tx.send(result).is_err() {
            warn!("failed to deliver {operation} response for msg_id {msg_id}");
        }
    } else {
        warn!("no waiting {operation} for msg_id {msg_id}");
    }
}

/// Generic worker client that manages a message-processing task
struct WorkerClient<M> {
    tx: Sender<M>,
    cancel: CancellationToken,
    tm: TaskTracker,
}

impl<M> Clone for WorkerClient<M> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            cancel: self.cancel.clone(),
            tm: self.tm.clone(),
        }
    }
}

impl<M> fmt::Debug for WorkerClient<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerClient").finish()
    }
}

impl<M> WorkerClient<M> {
    fn new(tx: Sender<M>, cancel: CancellationToken, tm: TaskTracker) -> Self {
        Self { tx, cancel, tm }
    }

    async fn send(&self, msg: M) -> Result<(), anyhow::Error>
    where
        M: Send,
    {
        self.tx
            .send(msg)
            .await
            .map_err(|e| anyhow::anyhow!("failed to send message to worker: {e}"))
    }

    async fn stop(&self) {
        self.cancel.cancel();
        self.tm.close();
        self.tm.wait().await;
    }
}

#[derive(Debug, Clone)]
pub struct KvClient<S> {
    worker: WorkerClient<KvMsg>,
    _marker: PhantomData<S>,
}

pub type SeqKvClient = KvClient<SeqStore>;
pub type LinKvClient = KvClient<LinStore>;
pub type LwwKvClient = KvClient<LwwStore>;

impl<S: StoreName> KvClient<S> {
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        id_provider: MsgIDProvider,
        node_id: String,
        tx_payload: Sender<Message<Body<KvPayload>>>,
    ) -> Self {
        let (tx, rx) = channel::<KvMsg>(1);
        let tm = TaskTracker::new();
        let cancel = CancellationToken::new();

        tm.spawn(Self::worker_loop(
            rx,
            cancel.clone(),
            node_id,
            id_provider.clone(),
            tx_payload,
        ));

        KvClient {
            worker: WorkerClient::new(tx, cancel, tm),
            _marker: PhantomData,
        }
    }

    async fn worker_loop(
        mut rx: tokio::sync::mpsc::Receiver<KvMsg>,
        cancel: CancellationToken,
        node_id: String,
        id_provider: MsgIDProvider,
        mut tx_payload: Sender<Message<Body<KvPayload>>>,
    ) {
        let mut waiters = KvWaiters {
            read: HashMap::new(),
            write: HashMap::new(),
            cas: HashMap::new(),
        };
        loop {
            select! {
                () = cancel.cancelled() => break,
                msg = rx.recv() => {
                    let Some(msg) = msg else {
                        break;
                    };
                    debug!(
                        msg_type = kv_msg_label(&msg),
                        waiting_reads = waiters.read.len(),
                        waiting_writes = waiters.write.len(),
                        waiting_cas = waiters.cas.len(),
                        "kv worker dequeued message"
                    );

                    if let Err(err) = KvHandler::new(
                        node_id.as_str(),
                        S::name(),
                        &id_provider,
                        &mut waiters,
                        &mut tx_payload,
                    )
                    .dispatch(msg)
                    .await
                    {
                        error!("failed to handle kv message: {err}");
                    }
                }
            }
        }
    }

    pub async fn read<T: DeserializeOwned>(&self, key: impl AsRef<str>) -> Result<T, KvError> {
        self.read_with_deadline(key.as_ref().to_string(), None)
            .await
    }

    pub async fn write<T: Serialize>(&self, key: impl AsRef<str>, value: T) -> Result<(), KvError> {
        let key = key.as_ref().to_string();
        let value = Self::serialize_value(value, "failed to serialize value for write")?;
        self.write_with_deadline(key, value, None).await
    }

    pub async fn compare_and_swap<T: Serialize>(
        &self,
        key: impl AsRef<str>,
        from: T,
        to: T,
        create_if_not_exists: bool,
    ) -> Result<bool, KvError> {
        let key = key.as_ref().to_string();
        let from = Self::serialize_value(from, "failed to serialize 'from' value for cas")?;
        let to = Self::serialize_value(to, "failed to serialize 'to' value for cas")?;
        self.cas_with_deadline(key, from, to, create_if_not_exists, None)
            .await
    }

    fn serialize_value<T: Serialize>(
        value: T,
        context: &'static str,
    ) -> Result<serde_json::Value, KvError> {
        serde_json::to_value(value)
            .context(context)
            .map_err(KvError::Internal)
    }

    async fn read_with_deadline<T: DeserializeOwned>(
        &self,
        key: String,
        deadline: Option<Duration>,
    ) -> Result<T, KvError> {
        let (tx, rx) = oneshot::channel();
        self.worker
            .send(KvMsg::Read(KvMsgRead { key, tx }))
            .await
            .map_err(KvError::Internal)?;
        let val = await_response_with_timeout(rx, "read", deadline).await?;
        serde_json::from_value(val).map_err(|err| KvError::Internal(anyhow::Error::new(err)))
    }

    async fn write_with_deadline(
        &self,
        key: String,
        value: serde_json::Value,
        deadline: Option<Duration>,
    ) -> Result<(), KvError> {
        let (tx, rx) = oneshot::channel();
        self.worker
            .send(KvMsg::Write(KvMsgWrite { key, value, tx }))
            .await
            .map_err(KvError::Internal)?;
        await_response_with_timeout(rx, "write", deadline).await
    }

    async fn cas_with_deadline(
        &self,
        key: String,
        from: serde_json::Value,
        to: serde_json::Value,
        create_if_not_exists: bool,
        deadline: Option<Duration>,
    ) -> Result<bool, KvError> {
        let (tx, rx) = oneshot::channel();
        self.worker
            .send(KvMsg::CmpAndSwp(KvMsgCmpAndSwp {
                key,
                from,
                to,
                create_if_not_exists,
                tx,
            }))
            .await
            .map_err(KvError::Internal)?;
        await_response_with_timeout(rx, "cas", deadline).await
    }

    pub async fn process(&self, msg: Message<Body<KvPayload>>) -> anyhow::Result<()> {
        let Message {
            src: _,
            dst: _,
            body:
                Body {
                    incoming_msg_id: _,
                    in_reply_to,
                    payload,
                },
        } = msg;
        if let Some(in_reply_to) = in_reply_to {
            payload.handle_kv(in_reply_to, &self.worker.tx).await?;
        }

        Ok(())
    }

    pub async fn stop(&self) {
        self.worker.stop().await;
    }
}

#[derive(Debug, Clone)]
pub struct TsoClient {
    worker: WorkerClient<TsoMsg>,
}

impl TsoClient {
    #[must_use]
    pub fn new(
        id_provider: MsgIDProvider,
        node_id: String,
        tx_payload: Sender<Message<Body<KvPayload>>>,
    ) -> Self {
        let (tx, rx) = channel::<TsoMsg>(1);
        let tm = TaskTracker::new();
        let cancel = CancellationToken::new();

        tm.spawn(Self::worker_loop(
            rx,
            cancel.clone(),
            node_id,
            id_provider,
            tx_payload,
        ));

        TsoClient {
            worker: WorkerClient::new(tx, cancel, tm),
        }
    }

    async fn worker_loop(
        mut rx: tokio::sync::mpsc::Receiver<TsoMsg>,
        cancel: CancellationToken,
        node_id: String,
        id_provider: MsgIDProvider,
        mut tx_payload: Sender<Message<Body<KvPayload>>>,
    ) {
        let mut waiting_for_ts = HashMap::<u64, oneshot::Sender<Result<u64, KvError>>>::new();
        loop {
            select! {
                () = cancel.cancelled() => break,
                msg = rx.recv() => {
                    let Some(msg): Option<TsoMsg> = msg else {
                        break;
                    };
                    let mut handler = TsoHandler {
                        node_id: node_id.as_str(),
                        id_provider: &id_provider,
                        waiting_for_ts: &mut waiting_for_ts,
                        tx_payload: &mut tx_payload,
                    };
                    if let Err(err) = msg.handle(&mut handler).await {
                        error!("failed to handle tso message: {err}");
                    }
                }
            }
        }
    }

    pub async fn ts(&self) -> Result<u64, KvError> {
        self.ts_with_deadline(None).await
    }

    async fn ts_with_deadline(&self, deadline: Option<Duration>) -> Result<u64, KvError> {
        let (tx, rx) = oneshot::channel();
        self.worker
            .send(TsoMsg::Ts(TsoMsgTs { tx }))
            .await
            .map_err(KvError::Internal)?;
        await_response_with_timeout(rx, "ts", deadline).await
    }

    pub async fn process(&self, msg: Message<Body<KvPayload>>) -> anyhow::Result<()> {
        let Message {
            body:
                Body {
                    in_reply_to,
                    payload,
                    ..
                },
            ..
        } = msg;
        if let Some(in_reply_to) = in_reply_to {
            payload.handle_tso(in_reply_to, &self.worker.tx).await?;
        }
        Ok(())
    }

    pub async fn stop(&self) {
        self.worker.stop().await;
    }
}

async fn await_response_with_timeout<T>(
    rx: oneshot::Receiver<Result<T, KvError>>,
    operation: &'static str,
    deadline: Option<Duration>,
) -> Result<T, KvError> {
    let recv_result = if let Some(limit) = deadline {
        match timeout(limit, rx).await {
            Ok(result) => result,
            Err(_) => {
                return Err(KvError::Timeout {
                    operation,
                    duration: limit,
                });
            }
        }
    } else {
        rx.await
    };

    recv_result
        .context(format!("failed to receive {operation} response"))
        .map_err(KvError::Internal)?
}

#[async_trait]
#[allow(dead_code)]
pub trait KvClientTimeoutExt<S: StoreName>: Send + Sync {
    async fn read_with_timeout<T>(&self, key: &str, timeout: Duration) -> Result<T, KvError>
    where
        T: DeserializeOwned + Send;

    async fn write_with_timeout<T>(
        &self,
        key: &str,
        value: T,
        timeout: Duration,
    ) -> Result<(), KvError>
    where
        T: Serialize + Send;

    async fn compare_and_swap_with_timeout<T>(
        &self,
        key: &str,
        from: T,
        to: T,
        create_if_not_exists: bool,
        timeout: Duration,
    ) -> Result<bool, KvError>
    where
        T: Serialize + Send;
}

#[async_trait]
impl<S> KvClientTimeoutExt<S> for KvClient<S>
where
    S: StoreName,
{
    async fn read_with_timeout<T>(&self, key: &str, timeout: Duration) -> Result<T, KvError>
    where
        T: DeserializeOwned + Send,
    {
        self.read_with_deadline(key.to_string(), Some(timeout))
            .await
    }

    async fn write_with_timeout<T>(
        &self,
        key: &str,
        value: T,
        timeout: Duration,
    ) -> Result<(), KvError>
    where
        T: Serialize + Send,
    {
        let value = Self::serialize_value(value, "failed to serialize value for write")?;
        self.write_with_deadline(key.to_string(), value, Some(timeout))
            .await
    }

    async fn compare_and_swap_with_timeout<T>(
        &self,
        key: &str,
        from: T,
        to: T,
        create_if_not_exists: bool,
        timeout: Duration,
    ) -> Result<bool, KvError>
    where
        T: Serialize + Send,
    {
        let from = Self::serialize_value(from, "failed to serialize 'from' value for cas")?;
        let to = Self::serialize_value(to, "failed to serialize 'to' value for cas")?;
        self.cas_with_deadline(
            key.to_string(),
            from,
            to,
            create_if_not_exists,
            Some(timeout),
        )
        .await
    }
}

#[async_trait]
#[allow(dead_code)]
pub trait TsoClientTimeoutExt: Send + Sync {
    async fn ts_with_timeout(&self, timeout: Duration) -> Result<u64, KvError>;
}

#[async_trait]
impl TsoClientTimeoutExt for TsoClient {
    async fn ts_with_timeout(&self, timeout: Duration) -> Result<u64, KvError> {
        self.ts_with_deadline(Some(timeout)).await
    }
}

pub(crate) struct KvWaiters {
    read: HashMap<u64, oneshot::Sender<Result<serde_json::Value, KvError>>>,
    write: HashMap<u64, oneshot::Sender<Result<(), KvError>>>,
    cas: HashMap<u64, oneshot::Sender<Result<bool, KvError>>>,
}

pub(crate) struct KvHandler<'a> {
    node_id: &'a str,
    store_name: &'a str,
    id_provider: &'a MsgIDProvider,
    waiters: &'a mut KvWaiters,
    tx_payload: &'a mut Sender<Message<Body<KvPayload>>>,
}

impl<'a> KvHandler<'a> {
    fn new(
        node_id: &'a str,
        store_name: &'a str,
        id_provider: &'a MsgIDProvider,
        waiters: &'a mut KvWaiters,
        tx_payload: &'a mut Sender<Message<Body<KvPayload>>>,
    ) -> Self {
        Self {
            node_id,
            store_name,
            id_provider,
            waiters,
            tx_payload,
        }
    }

    async fn dispatch(&mut self, msg: KvMsg) -> anyhow::Result<()> {
        msg.handle(self).await
    }

    fn mk_error(code: u16, text: &str) -> KvError {
        match code {
            ERROR_CODE_KEY_NOT_EXIST => KvError::KeyDoesNotExist,
            ERROR_CODE_PRECONDITION_FAILED => KvError::PreconditionFailed,
            _ => KvError::Other {
                code,
                text: text.to_string(),
            },
        }
    }

    fn make_store_message(&self, payload: KvPayload, msg_id: u64) -> Message<Body<KvPayload>> {
        Message {
            src: self.node_id.to_owned(),
            dst: self.store_name.to_owned(),
            body: Body {
                incoming_msg_id: Some(msg_id),
                in_reply_to: None,
                payload,
            },
        }
    }
}

impl KvMsg {
    async fn handle(self, handler: &mut KvHandler<'_>) -> anyhow::Result<()> {
        match self {
            KvMsg::Read(msg) => msg.handle(handler).await,
            KvMsg::ReadResponse(msg) => {
                msg.handle(handler);
                Ok(())
            }
            KvMsg::Write(msg) => msg.handle(handler).await,
            KvMsg::WriteResponse(msg) => {
                msg.handle(handler);
                Ok(())
            }
            KvMsg::CmpAndSwp(msg) => msg.handle(handler).await,
            KvMsg::CmpAndSwpResponse(msg) => {
                msg.handle(handler);
                Ok(())
            }
            KvMsg::ErrorResponse(msg) => {
                msg.handle(handler);
                Ok(())
            }
        }
    }
}

impl KvMsgRead {
    async fn handle(self, handler: &mut KvHandler<'_>) -> anyhow::Result<()> {
        let msg_id = handler.id_provider.id();
        handler.waiters.read.insert(msg_id, self.tx);
        let waiting_reads = handler.waiters.read.len();
        debug!(msg_id, waiting_reads, "kv sending read request");

        let msg = handler.make_store_message(KvPayload::Read(Read { key: self.key }), msg_id);
        handler
            .tx_payload
            .send(msg)
            .await
            .context("failed to send read msg")
    }
}

impl KvMsgWrite {
    async fn handle(self, handler: &mut KvHandler<'_>) -> anyhow::Result<()> {
        let msg_id = handler.id_provider.id();
        handler.waiters.write.insert(msg_id, self.tx);
        let waiting_writes = handler.waiters.write.len();
        debug!(msg_id, waiting_writes, "kv sending write request");

        let msg = handler.make_store_message(
            KvPayload::Write(Write {
                key: self.key,
                value: self.value,
            }),
            msg_id,
        );
        handler
            .tx_payload
            .send(msg)
            .await
            .context("failed to send write msg")
    }
}

impl KvMsgCmpAndSwp {
    async fn handle(self, handler: &mut KvHandler<'_>) -> anyhow::Result<()> {
        let msg_id = handler.id_provider.id();
        handler.waiters.cas.insert(msg_id, self.tx);
        let waiting_cas = handler.waiters.cas.len();
        debug!(
            msg_id,
            waiting_cas,
            from = self.from.to_string(),
            to = self.to.to_string(),
            "kv sending cas request"
        );

        let msg = handler.make_store_message(
            KvPayload::Cas(Cas {
                key: self.key,
                from: self.from,
                to: self.to,
                create_if_not_exists: self.create_if_not_exists,
            }),
            msg_id,
        );
        handler
            .tx_payload
            .send(msg)
            .await
            .context("failed to send cmp_and_swp msg")
    }
}

impl KvMsgReadResponse {
    fn handle(self, handler: &mut KvHandler<'_>) {
        deliver_response(
            &mut handler.waiters.read,
            self.msg_id,
            Ok(self.value),
            "read",
        );
    }
}

impl KvMsgWriteResponse {
    fn handle(self, handler: &mut KvHandler<'_>) {
        deliver_response(&mut handler.waiters.write, self.msg_id, Ok(()), "write");
        debug!(
            msg_id = self.msg_id,
            waiting_writes = handler.waiters.write.len(),
            "kv delivered write response"
        );
    }
}

impl KvMsgCmpAndSwpResponse {
    fn handle(self, handler: &mut KvHandler<'_>) {
        deliver_response(
            &mut handler.waiters.cas,
            self.msg_id,
            Ok(self.swapped),
            "cas",
        );
        debug!(
            msg_id = self.msg_id,
            waiting_cas = handler.waiters.cas.len(),
            swapped = self.swapped,
            "kv delivered cas response"
        );
    }
}

impl KvMsgErrorResponse {
    fn handle(self, handler: &mut KvHandler<'_>) {
        let error = KvHandler::mk_error(self.code, &self.text);

        if handler.waiters.read.contains_key(&self.msg_id) {
            deliver_response(
                &mut handler.waiters.read,
                self.msg_id,
                Err(error),
                "read error",
            );
            debug!(
                msg_id = self.msg_id,
                waiting_reads = handler.waiters.read.len(),
                code = self.code,
                "kv delivered read error"
            );
        } else if handler.waiters.write.contains_key(&self.msg_id) {
            deliver_response(
                &mut handler.waiters.write,
                self.msg_id,
                Err(error),
                "write error",
            );
            debug!(
                msg_id = self.msg_id,
                waiting_writes = handler.waiters.write.len(),
                code = self.code,
                "kv delivered write error"
            );
        } else if handler.waiters.cas.contains_key(&self.msg_id) {
            if self.code == ERROR_CODE_PRECONDITION_FAILED {
                deliver_response(
                    &mut handler.waiters.cas,
                    self.msg_id,
                    Ok(false),
                    "cas mismatch",
                );
                debug!(
                    waiting_cas = handler.waiters.cas.len(),
                    code = self.code,
                    "kv delivered cas mismatch"
                );
            } else {
                deliver_response(
                    &mut handler.waiters.cas,
                    self.msg_id,
                    Err(error),
                    "cas error",
                );
                debug!(
                    waiting_cas = handler.waiters.cas.len(),
                    code = self.code,
                    "kv delivered cas error"
                );
            }
        } else {
            warn!(
                waiting_reads = handler.waiters.read.len(),
                waiting_writes = handler.waiters.write.len(),
                waiting_cas = handler.waiters.cas.len(),
                "received kv error response for unknown msg_id {}: code={} text={}",
                self.msg_id,
                self.code,
                self.text
            );
        }
    }
}

pub(crate) struct TsoHandler<'a> {
    node_id: &'a str,
    id_provider: &'a MsgIDProvider,
    waiting_for_ts: &'a mut HashMap<u64, oneshot::Sender<Result<u64, KvError>>>,
    tx_payload: &'a mut Sender<Message<Body<KvPayload>>>,
}

impl TsoHandler<'_> {
    fn make_message(&self, payload: KvPayload, msg_id: u64) -> Message<Body<KvPayload>> {
        Message {
            src: self.node_id.to_owned(),
            dst: "lin-tso".to_string(),
            body: Body {
                incoming_msg_id: Some(msg_id),
                in_reply_to: None,
                payload,
            },
        }
    }

    async fn request_timestamp(
        &mut self,
        tx: oneshot::Sender<Result<u64, KvError>>,
    ) -> anyhow::Result<()> {
        let msg_id = self.id_provider.id();
        self.waiting_for_ts.insert(msg_id, tx);
        let msg = self.make_message(KvPayload::Ts(Ts), msg_id);
        self.tx_payload
            .send(msg)
            .await
            .context("failed to send ts msg")
    }

    fn deliver_timestamp(&mut self, msg_id: u64, ts: u64) {
        deliver_response(self.waiting_for_ts, msg_id, Ok(ts), "ts");
    }
}

impl TsoMsg {
    async fn handle(self, handler: &mut TsoHandler<'_>) -> anyhow::Result<()> {
        match self {
            TsoMsg::Ts(msg) => msg.handle(handler).await,
            TsoMsg::TsOk(msg) => {
                msg.handle(handler);
                Ok(())
            }
        }
    }
}

impl TsoMsgTs {
    async fn handle(self, handler: &mut TsoHandler<'_>) -> anyhow::Result<()> {
        handler.request_timestamp(self.tx).await
    }
}

impl TsoMsgTsOk {
    fn handle(self, handler: &mut TsoHandler<'_>) {
        handler.deliver_timestamp(self.msg_id, self.ts);
    }
}

fn kv_msg_label(msg: &KvMsg) -> &'static str {
    match msg {
        KvMsg::Read(_) => "read",
        KvMsg::ReadResponse(_) => "read_response",
        KvMsg::Write(_) => "write",
        KvMsg::WriteResponse(_) => "write_response",
        KvMsg::CmpAndSwp(_) => "cas",
        KvMsg::CmpAndSwpResponse(_) => "cas_response",
        KvMsg::ErrorResponse(_) => "error_response",
    }
}
