use anyhow::{Context, Error, bail};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::select;
use tokio::sync::mpsc::{Sender, channel};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, warn};

use crate::message::{Body, Message};

#[derive(Debug, Clone, Default)]
pub struct MsgIDProvider {
    inner: Arc<AtomicU64>,
}

impl MsgIDProvider {
    pub fn new() -> MsgIDProvider {
        MsgIDProvider {
            inner: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn id(&self) -> u64 {
        self.inner.fetch_add(1, Ordering::SeqCst)
    }
}

#[derive(Debug, Clone)]
pub struct KvClient {
    tx: Sender<KVMsg>,
    cancel: CancellationToken,
    tm: TaskTracker,
}

enum KVMsg {
    ShouldProcess {
        msg_id: u64,
        tx: oneshot::Sender<bool>,
    },
    Read {
        key: String,
        tx: oneshot::Sender<anyhow::Result<u64>>,
    },
    ReadResponse {
        msg_id: u64,
        value: u64,
    },
    Write {
        key: String,
        value: u64,
        tx: oneshot::Sender<anyhow::Result<()>>,
    },
    WriteResponse {
        msg_id: u64,
    },
    CmpAndSwp {
        key: String,
        from: u64,
        to: u64,
        create_if_not_exists: bool,
        tx: oneshot::Sender<anyhow::Result<bool>>,
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

impl KvClient {
    pub fn new(
        id_provider: MsgIDProvider,
        node_id: String,
        mut tx_payload: Sender<Message<Body<KvPayload>>>,
    ) -> KvClient {
        let (tx, mut rx) = channel(1);
        let tm = TaskTracker::new();
        let cancel = CancellationToken::new();
        tm.spawn({
            let cancel = cancel.clone();
            async move {
                let mut waiting_for_read =
                    HashMap::<u64, oneshot::Sender<anyhow::Result<u64>>>::new();
                let mut waiting_for_write =
                    HashMap::<u64, oneshot::Sender<anyhow::Result<()>>>::new();
                let mut waiting_for_cas =
                    HashMap::<u64, oneshot::Sender<anyhow::Result<bool>>>::new();
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
                            if let Err(err) = handle_message(msg, &node_id, &id_provider, &mut waiting_for_read, &mut waiting_for_write, &mut waiting_for_cas, &mut tx_payload).await {
                                error!("failed to handle kv message: {err}");
                            };
                        }
                    }
                }
            }
        });
        KvClient { tx, cancel, tm }
    }

    pub async fn should_process(&self, msg_id: u64) -> anyhow::Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(KVMsg::ShouldProcess { msg_id, tx })
            .await
            .context("failed to send msg")?;
        rx.await.context("failed to receive")
    }

    pub async fn read(&self, key: impl AsRef<str>) -> anyhow::Result<u64> {
        let key = key.as_ref().to_string();
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(KVMsg::Read { key, tx })
            .await
            .context("failed to send msg")?;
        rx.await.context("failed to receive")?
    }

    pub async fn write(&self, key: impl AsRef<str>, value: u64) -> anyhow::Result<()> {
        let key = key.as_ref().to_string();
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(KVMsg::Write { key, value, tx })
            .await
            .context("failed to send msg")?;
        rx.await.context("failed to receive")?
    }

    pub async fn compare_and_swap(
        &self,
        key: impl AsRef<str>,
        from: u64,
        to: u64,
        create_if_not_exists: bool,
    ) -> anyhow::Result<bool> {
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
            .context("failed to send msg")?;
        rx.await.context("failed to receive")?
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
    id_provider: &MsgIDProvider,
    waiting_for_read: &mut HashMap<u64, oneshot::Sender<anyhow::Result<u64>>>,
    waiting_for_write: &mut HashMap<u64, oneshot::Sender<anyhow::Result<()>>>,
    waiting_for_cas: &mut HashMap<u64, oneshot::Sender<anyhow::Result<bool>>>,
    tx_payload: &mut Sender<Message<Body<KvPayload>>>,
) -> anyhow::Result<()> {
    match msg {
        KVMsg::ShouldProcess { msg_id, tx } => {
            let res = waiting_for_read.contains_key(&msg_id)
                || waiting_for_write.contains_key(&msg_id)
                || waiting_for_cas.contains_key(&msg_id);
            debug!(msg_id, res, "kv should_process check");
            if let Err(err) = tx.send(res) {
                warn!("should_process receiver dropped for msg_id {msg_id}: {err}");
            }
        }
        KVMsg::Read { key, tx } => {
            let msg_id = id_provider.id();
            waiting_for_read.insert(msg_id, tx);
            let waiting_reads = waiting_for_read.len();
            let msg = Message {
                src: node_id.clone(),
                dst: "seq-kv".to_string(),
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
                dst: "seq-kv".to_string(),
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
                dst: "seq-kv".to_string(),
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
            let err_msg = format!("kv error code {code}: {text}");
            if let Some(tx) = waiting_for_read.remove(&msg_id) {
                if let Err(err) = tx.send(Err(Error::msg(err_msg.clone()))) {
                    warn!("failed to deliver read error for msg_id {msg_id}: {err:?}");
                }
                debug!(
                    msg_id,
                    waiting_reads = waiting_for_read.len(),
                    code,
                    "kv delivered read error"
                );
            } else if let Some(tx) = waiting_for_write.remove(&msg_id) {
                if let Err(err) = tx.send(Err(Error::msg(err_msg.clone()))) {
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
                } else if let Err(err) = tx.send(Err(Error::msg(err_msg.clone()))) {
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
                    "received kv error response for unknown msg_id {msg_id}: {err_msg}"
                );
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
}

fn kv_msg_label(msg: &KVMsg) -> &'static str {
    match msg {
        KVMsg::ShouldProcess { .. } => "should_process",
        KVMsg::Read { .. } => "read",
        KVMsg::ReadResponse { .. } => "read_response",
        KVMsg::Write { .. } => "write",
        KVMsg::WriteResponse { .. } => "write_response",
        KVMsg::CmpAndSwp { .. } => "cas",
        KVMsg::CmpAndSwpResponse { .. } => "cas_response",
        KVMsg::ErrorResponse { .. } => "error_response",
    }
}
