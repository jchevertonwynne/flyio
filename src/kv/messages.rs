use serde_json::Value;
use tokio::sync::oneshot;

use super::types::KvError;

#[derive(Debug)]
pub(crate) struct KvMsgRead {
    pub(crate) key: String,
    pub(crate) tx: oneshot::Sender<Result<Value, KvError>>,
}

#[derive(Debug)]
pub(crate) struct KvMsgReadResponse {
    pub(crate) msg_id: u64,
    pub(crate) value: Value,
}

#[derive(Debug)]
pub(crate) struct KvMsgWrite {
    pub(crate) key: String,
    pub(crate) value: Value,
    pub(crate) tx: oneshot::Sender<Result<(), KvError>>,
}

#[derive(Debug)]
pub(crate) struct KvMsgWriteResponse {
    pub(crate) msg_id: u64,
}

#[derive(Debug)]
pub(crate) struct KvMsgCmpAndSwp {
    pub(crate) key: String,
    pub(crate) from: Value,
    pub(crate) to: Value,
    pub(crate) create_if_not_exists: bool,
    pub(crate) tx: oneshot::Sender<Result<bool, KvError>>,
}

#[derive(Debug)]
pub(crate) struct KvMsgCmpAndSwpResponse {
    pub(crate) msg_id: u64,
    pub(crate) swapped: bool,
}

#[derive(Debug)]
pub(crate) struct KvMsgErrorResponse {
    pub(crate) msg_id: u64,
    pub(crate) code: u16,
    pub(crate) text: String,
}

#[derive(Debug)]
pub(crate) enum KvMsg {
    Read(KvMsgRead),
    ReadResponse(KvMsgReadResponse),
    Write(KvMsgWrite),
    WriteResponse(KvMsgWriteResponse),
    CmpAndSwp(KvMsgCmpAndSwp),
    CmpAndSwpResponse(KvMsgCmpAndSwpResponse),
    ErrorResponse(KvMsgErrorResponse),
}

#[derive(Debug)]
pub(crate) struct TsoMsgTs {
    pub(crate) tx: oneshot::Sender<Result<u64, KvError>>,
}

#[derive(Debug)]
pub(crate) struct TsoMsgTsOk {
    pub(crate) msg_id: u64,
    pub(crate) ts: u64,
}

#[derive(Debug)]
pub(crate) enum TsoMsg {
    Ts(TsoMsgTs),
    TsOk(TsoMsgTsOk),
}
