use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::select;
use tokio::sync::mpsc::{Sender, channel};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, warn};

use crate::message::{Body, Message};

pub trait StoreName {
    fn name() -> &'static str;
}

#[derive(Debug, Clone)]
pub struct SeqStore;
impl StoreName for SeqStore {
    fn name() -> &'static str {
        "seq-kv"
    }
}

#[derive(Debug, Clone)]
pub struct LinStore;
impl StoreName for LinStore {
    fn name() -> &'static str {
        "lin-kv"
    }
}

#[derive(Debug, Clone)]
pub struct LwwStore;
impl StoreName for LwwStore {
    fn name() -> &'static str {
        "lww-kv"
    }
}

#[derive(Debug)]
pub enum KvError {
    KeyDoesNotExist,
    PreconditionFailed,
    Other { code: u16, text: String },
    Internal(anyhow::Error),
}

impl std::fmt::Display for KvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KvError::KeyDoesNotExist => write!(f, "key does not exist"),
            KvError::PreconditionFailed => write!(f, "precondition failed"),
            KvError::Other { code, text } => write!(f, "kv error code {code}: {text}"),
            KvError::Internal(err) => write!(f, "kv internal error: {err}"),
        }
    }
}

impl std::error::Error for KvError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            KvError::Internal(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct MsgIDProvider {
    inner: Arc<AtomicU64>,
    hook: Option<Arc<dyn Fn(u64) + Send + Sync + 'static>>,
}

impl fmt::Debug for MsgIDProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MsgIDProvider").finish()
    }
}

impl Default for MsgIDProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl MsgIDProvider {
    pub fn new() -> MsgIDProvider {
        MsgIDProvider {
            inner: Arc::new(AtomicU64::new(0)),
            hook: None,
        }
    }

    pub fn with_hook<F>(&self, hook: F) -> MsgIDProvider
    where
        F: Fn(u64) + Send + Sync + 'static,
    {
        MsgIDProvider {
            inner: Arc::clone(&self.inner),
            hook: Some(Arc::new(hook)),
        }
    }

    pub fn id(&self) -> u64 {
        let id = self.inner.fetch_add(1, Ordering::SeqCst);
        if let Some(hook) = &self.hook {
            hook(id);
        }
        id
    }
}

#[derive(Debug, Clone)]
pub struct KvClient<S> {
    tx: Sender<KVMsg>,
    cancel: CancellationToken,
    tm: TaskTracker,
    _marker: PhantomData<S>,
}

pub type SeqKvClient = KvClient<SeqStore>;
pub type LinKvClient = KvClient<LinStore>;
pub type LwwKvClient = KvClient<LwwStore>;

#[derive(Debug)]
enum KVMsg {
    Read {
        key: String,
        tx: oneshot::Sender<Result<u64, KvError>>,
    },
    ReadResponse {
        msg_id: u64,
        value: u64,
    },
    Write {
        key: String,
        value: u64,
        tx: oneshot::Sender<Result<(), KvError>>,
    },
    WriteResponse {
        msg_id: u64,
    },
    CmpAndSwp {
        key: String,
        from: u64,
        to: u64,
        create_if_not_exists: bool,
        tx: oneshot::Sender<Result<bool, KvError>>,
    },
    CmpAndSwpResponse {
        msg_id: u64,
        swapped: bool,
    },
    ErrorResponse {
        msg_id: u64,
        code: u16,
        text: String,
    },
}

impl<S: StoreName + Send + Sync + 'static> KvClient<S> {
    pub fn new(
        id_provider: MsgIDProvider,
        node_id: String,
        mut tx_payload: Sender<Message<Body<KvPayload>>>,
    ) -> Self {
        let (tx, mut rx) = channel(1);
        let tm = TaskTracker::new();
        let cancel = CancellationToken::new();
        tm.spawn({
            let cancel = cancel.clone();
            async move {
                let mut waiting_for_read =
                    HashMap::<u64, oneshot::Sender<Result<u64, KvError>>>::new();
                let mut waiting_for_write =
                    HashMap::<u64, oneshot::Sender<Result<(), KvError>>>::new();
                let mut waiting_for_cas =
                    HashMap::<u64, oneshot::Sender<Result<bool, KvError>>>::new();
                loop {
                    select! {
                        _ = cancel.cancelled() => break,
                        msg = rx.recv() => {
                            let Some(msg) = msg else {
                                break;
                            };
                              debug!(
                                  msg_type = kv_msg_label(&msg),
                                  waiting_reads = waiting_for_read.len(),
                                  waiting_writes = waiting_for_write.len(),
                                  waiting_cas = waiting_for_cas.len(),
                                  "kv worker dequeued message"
                              );
                            if let Err(err) = handle_message(msg, &node_id, S::name(), &id_provider, &mut waiting_for_read, &mut waiting_for_write, &mut waiting_for_cas, &mut tx_payload).await {
                                error!("failed to handle kv message: {err}");
                            };
                        }
                    }
                }
            }
        });
        KvClient {
            tx,
            cancel,
            tm,
            _marker: PhantomData,
        }
    }

    pub async fn read(&self, key: impl AsRef<str>) -> Result<u64, KvError> {
        let key = key.as_ref().to_string();
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(KVMsg::Read { key, tx })
            .await
            .context("failed to send read msg")
            .map_err(KvError::Internal)?;
        rx.await
            .context("failed to receive read response")
            .map_err(KvError::Internal)?
    }

    pub async fn write(&self, key: impl AsRef<str>, value: u64) -> Result<(), KvError> {
        let key = key.as_ref().to_string();
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(KVMsg::Write { key, value, tx })
            .await
            .context("failed to send write msg")
            .map_err(KvError::Internal)?;
        rx.await
            .context("failed to receive write response")
            .map_err(KvError::Internal)?
    }

    pub async fn compare_and_swap(
        &self,
        key: impl AsRef<str>,
        from: u64,
        to: u64,
        create_if_not_exists: bool,
    ) -> Result<bool, KvError> {
        let key = key.as_ref().to_string();
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(KVMsg::CmpAndSwp {
                key,
                from,
                to,
                create_if_not_exists,
                tx,
            })
            .await
            .context("failed to send cas msg")
            .map_err(KvError::Internal)?;
        rx.await
            .context("failed to receive cas response")
            .map_err(KvError::Internal)?
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
            match payload {
                KvPayload::ReadOk { value } => {
                    self.tx
                        .send(KVMsg::ReadResponse {
                            msg_id: in_reply_to,
                            value,
                        })
                        .await
                        .context("failed to send read response")?;
                }
                KvPayload::WriteOk => {
                    self.tx
                        .send(KVMsg::WriteResponse {
                            msg_id: in_reply_to,
                        })
                        .await
                        .context("failed to send write response")?;
                }
                KvPayload::CasOk => {
                    self.tx
                        .send(KVMsg::CmpAndSwpResponse {
                            msg_id: in_reply_to,
                            swapped: true,
                        })
                        .await
                        .context("failed to send cas response")?;
                }
                KvPayload::Error { code, text } => {
                    self.tx
                        .send(KVMsg::ErrorResponse {
                            msg_id: in_reply_to,
                            code,
                            text,
                        })
                        .await
                        .context("failed to send error response")?;
                }
                _ => {
                    bail!("unexpected kv payload: {:?}", payload);
                }
            }
        }

        Ok(())
    }

    pub async fn stop(&self) {
        self.cancel.cancel();
        self.tm.close();
        self.tm.wait().await;
    }
}

async fn handle_message(
    msg: KVMsg,
    node_id: &String,
    store_name: &str,
    id_provider: &MsgIDProvider,
    waiting_for_read: &mut HashMap<u64, oneshot::Sender<Result<u64, KvError>>>,
    waiting_for_write: &mut HashMap<u64, oneshot::Sender<Result<(), KvError>>>,
    waiting_for_cas: &mut HashMap<u64, oneshot::Sender<Result<bool, KvError>>>,
    tx_payload: &mut Sender<Message<Body<KvPayload>>>,
) -> anyhow::Result<()> {
    match msg {
        KVMsg::Read { key, tx } => {
            let msg_id = id_provider.id();
            waiting_for_read.insert(msg_id, tx);
            let waiting_reads = waiting_for_read.len();
            let msg = Message {
                src: node_id.clone(),
                dst: store_name.to_string(),
                body: Body {
                    incoming_msg_id: Some(msg_id),
                    in_reply_to: None,
                    payload: KvPayload::Read { key },
                },
            };
            debug!(msg_id, waiting_reads, "kv sending read request");
            tx_payload
                .send(msg)
                .await
                .context("failed to send read msg")?;
        }
        KVMsg::Write { key, value, tx } => {
            let msg_id = id_provider.id();
            waiting_for_write.insert(msg_id, tx);
            let waiting_writes = waiting_for_write.len();
            let msg = Message {
                src: node_id.clone(),
                dst: store_name.to_string(),
                body: Body {
                    incoming_msg_id: Some(msg_id),
                    in_reply_to: None,
                    payload: KvPayload::Write { key, value },
                },
            };
            debug!(msg_id, waiting_writes, "kv sending write request");
            tx_payload
                .send(msg)
                .await
                .context("failed to send write msg")?;
        }
        KVMsg::CmpAndSwp {
            key,
            from,
            to,
            create_if_not_exists,
            tx,
        } => {
            let msg_id = id_provider.id();
            waiting_for_cas.insert(msg_id, tx);
            let waiting_cas = waiting_for_cas.len();
            let msg = Message {
                src: node_id.clone(),
                dst: store_name.to_string(),
                body: Body {
                    incoming_msg_id: Some(msg_id),
                    in_reply_to: None,
                    payload: KvPayload::Cas {
                        key,
                        from,
                        to,
                        create_if_not_exists,
                    },
                },
            };
            debug!(msg_id, waiting_cas, from, to, "kv sending cas request");
            tx_payload
                .send(msg)
                .await
                .context("failed to send cmp_and_swp msg")?;
        }
        KVMsg::ReadResponse { msg_id, value } => {
            if let Some(tx) = waiting_for_read.remove(&msg_id) {
                if let Err(err) = tx.send(Ok(value)) {
                    warn!("failed to deliver read response for msg_id {msg_id}: {err:?}");
                }
                debug!(
                    msg_id,
                    waiting_reads = waiting_for_read.len(),
                    value,
                    "kv delivered read response"
                );
            } else {
                warn!("no waiting read for msg_id {msg_id}");
            }
        }
        KVMsg::WriteResponse { msg_id } => {
            if let Some(tx) = waiting_for_write.remove(&msg_id) {
                if let Err(err) = tx.send(Ok(())) {
                    warn!("failed to deliver write response for msg_id {msg_id}: {err:?}");
                }
                debug!(
                    msg_id,
                    waiting_writes = waiting_for_write.len(),
                    "kv delivered write response"
                );
            } else {
                warn!("no waiting write for msg_id {msg_id}");
            }
        }
        KVMsg::CmpAndSwpResponse { msg_id, swapped } => {
            if let Some(tx) = waiting_for_cas.remove(&msg_id) {
                if let Err(err) = tx.send(Ok(swapped)) {
                    warn!("failed to deliver cas response for msg_id {msg_id}: {err:?}");
                }
                debug!(
                    msg_id,
                    waiting_cas = waiting_for_cas.len(),
                    swapped,
                    "kv delivered cas response"
                );
            } else {
                warn!("no waiting cas for msg_id {msg_id}");
            }
        }
        KVMsg::ErrorResponse { msg_id, code, text } => {
            let mk_error = |t: &str| match code {
                20 => KvError::KeyDoesNotExist,
                22 => KvError::PreconditionFailed,
                _ => KvError::Other {
                    code,
                    text: t.to_string(),
                },
            };

            if let Some(tx) = waiting_for_read.remove(&msg_id) {
                if let Err(err) = tx.send(Err(mk_error(&text))) {
                    warn!("failed to deliver read error for msg_id {msg_id}: {err:?}");
                }
                debug!(
                    msg_id,
                    waiting_reads = waiting_for_read.len(),
                    code,
                    "kv delivered read error"
                );
            } else if let Some(tx) = waiting_for_write.remove(&msg_id) {
                if let Err(err) = tx.send(Err(mk_error(&text))) {
                    warn!("failed to deliver write error for msg_id {msg_id}: {err:?}");
                }
                debug!(
                    msg_id,
                    waiting_writes = waiting_for_write.len(),
                    code,
                    "kv delivered write error"
                );
            } else if let Some(tx) = waiting_for_cas.remove(&msg_id) {
                if code == 22 {
                    if let Err(err) = tx.send(Ok(false)) {
                        warn!("failed to deliver cas mismatch result for msg_id {msg_id}: {err:?}");
                    }
                    debug!(
                        msg_id,
                        waiting_cas = waiting_for_cas.len(),
                        code,
                        "kv delivered cas mismatch"
                    );
                } else if let Err(err) = tx.send(Err(mk_error(&text))) {
                    warn!("failed to deliver cas error for msg_id {msg_id}: {err:?}");
                } else {
                    debug!(
                        msg_id,
                        waiting_cas = waiting_for_cas.len(),
                        code,
                        "kv delivered cas error"
                    );
                }
            } else {
                warn!(
                    waiting_reads = waiting_for_read.len(),
                    waiting_writes = waiting_for_write.len(),
                    waiting_cas = waiting_for_cas.len(),
                    "received kv error response for unknown msg_id {msg_id}: code={code} text={text}"
                );
            }
        }
    }
    
    Ok(())
}

#[derive(Debug, Clone)]
pub struct TsoClient {
    tx: Sender<TsoMsg>,
    cancel: CancellationToken,
    tm: TaskTracker,
}

#[derive(Debug)]
enum TsoMsg {
    Ts {
        tx: oneshot::Sender<Result<u64, KvError>>,
    },
    TsOk {
        msg_id: u64,
        ts: u64,
    },
}

impl TsoClient {
    pub fn new(
        id_provider: MsgIDProvider,
        node_id: String,
        mut tx_payload: Sender<Message<Body<KvPayload>>>,
    ) -> Self {
        let (tx, mut rx) = channel(1);
        let tm = TaskTracker::new();
        let cancel = CancellationToken::new();
        tm.spawn({
            let cancel = cancel.clone();
            async move {
                let mut waiting_for_ts =
                    HashMap::<u64, oneshot::Sender<Result<u64, KvError>>>::new();
                loop {
                    select! {
                        _ = cancel.cancelled() => break,
                        msg = rx.recv() => {
                            let Some(msg) = msg else {
                                break;
                            };
                            if let Err(err) = handle_tso_message(msg, &node_id, &id_provider, &mut waiting_for_ts, &mut tx_payload).await {
                                error!("failed to handle tso message: {err}");
                            };
                        }
                    }
                }
            }
        });
        TsoClient { tx, cancel, tm }
    }

    pub async fn ts(&self) -> Result<u64, KvError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(TsoMsg::Ts { tx })
            .await
            .context("failed to send ts msg")
            .map_err(KvError::Internal)?;
        rx.await
            .context("failed to receive ts response")
            .map_err(KvError::Internal)?
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
            match payload {
                KvPayload::TsOk { ts } => {
                    self.tx
                        .send(TsoMsg::TsOk {
                            msg_id: in_reply_to,
                            ts,
                        })
                        .await
                        .context("failed to send ts response")?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub async fn stop(&self) {
        self.cancel.cancel();
        self.tm.close();
        self.tm.wait().await;
    }
}

async fn handle_tso_message(
    msg: TsoMsg,
    node_id: &String,
    id_provider: &MsgIDProvider,
    waiting_for_ts: &mut HashMap<u64, oneshot::Sender<Result<u64, KvError>>>,
    tx_payload: &mut Sender<Message<Body<KvPayload>>>,
) -> anyhow::Result<()> {
    match msg {
        TsoMsg::Ts { tx } => {
            let msg_id = id_provider.id();
            waiting_for_ts.insert(msg_id, tx);
            let msg = Message {
                src: node_id.clone(),
                dst: "lin-tso".to_string(),
                body: Body {
                    incoming_msg_id: Some(msg_id),
                    in_reply_to: None,
                    payload: KvPayload::Ts,
                },
            };
            tx_payload
                .send(msg)
                .await
                .context("failed to send ts msg")?;
        }
        TsoMsg::TsOk { msg_id, ts } => {
            if let Some(tx) = waiting_for_ts.remove(&msg_id) {
                if let Err(_) = tx.send(Ok(ts)) {
                    warn!("failed to deliver ts response");
                }
            } else {
                warn!("no waiting ts for msg_id {msg_id}");
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KvPayload {
    Read {
        key: String,
    },
    ReadOk {
        value: u64,
    },
    Write {
        key: String,
        value: u64,
    },
    WriteOk,
    Cas {
        key: String,
        from: u64,
        to: u64,
        create_if_not_exists: bool,
    },
    CasOk,
    Error {
        code: u16,
        text: String,
    },
    Ts,
    TsOk {
        ts: u64,
    },
}

fn kv_msg_label(msg: &KVMsg) -> &'static str {
    match msg {
        KVMsg::Read { .. } => "read",
        KVMsg::ReadResponse { .. } => "read_response",
        KVMsg::Write { .. } => "write",
        KVMsg::WriteResponse { .. } => "write_response",
        KVMsg::CmpAndSwp { .. } => "cas",
        KVMsg::CmpAndSwpResponse { .. } => "cas_response",
        KVMsg::ErrorResponse { .. } => "error_response",
    }
}
