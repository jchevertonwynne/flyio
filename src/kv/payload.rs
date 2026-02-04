use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

use super::messages::{
    KvMsg, KvMsgCmpAndSwpResponse, KvMsgErrorResponse, KvMsgReadResponse, KvMsgWriteResponse,
    TsoMsg, TsoMsgTsOk,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Read {
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ReadOk {
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Write {
    pub key: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WriteOk;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Cas {
    pub key: String,
    pub from: serde_json::Value,
    pub to: serde_json::Value,
    pub create_if_not_exists: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CasOk;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Error {
    pub code: u16,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Ts;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TsOk {
    pub ts: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KvPayload {
    Read(Read),
    ReadOk(ReadOk),
    Write(Write),
    WriteOk(WriteOk),
    Cas(Cas),
    CasOk(CasOk),
    Error(Error),
    Ts(Ts),
    TsOk(TsOk),
}

impl HandleKvPayload for KvPayload {
    async fn handle_kv(self, reply_id: u64, tx: &Sender<KvMsg>) -> anyhow::Result<()> {
        match self {
            KvPayload::Read(p) => p.handle_kv(reply_id, tx).await,
            KvPayload::ReadOk(p) => p.handle_kv(reply_id, tx).await,
            KvPayload::Write(p) => p.handle_kv(reply_id, tx).await,
            KvPayload::WriteOk(p) => p.handle_kv(reply_id, tx).await,
            KvPayload::Cas(p) => p.handle_kv(reply_id, tx).await,
            KvPayload::CasOk(p) => p.handle_kv(reply_id, tx).await,
            KvPayload::Error(p) => p.handle_kv(reply_id, tx).await,
            KvPayload::Ts(p) => p.handle_kv(reply_id, tx).await,
            KvPayload::TsOk(p) => p.handle_kv(reply_id, tx).await,
        }
    }
}

impl HandleTsoPayload for KvPayload {
    async fn handle_tso(self, reply_id: u64, tx: &Sender<TsoMsg>) -> anyhow::Result<()> {
        match self {
            KvPayload::Read(p) => p.handle_tso(reply_id, tx).await,
            KvPayload::ReadOk(p) => p.handle_tso(reply_id, tx).await,
            KvPayload::Write(p) => p.handle_tso(reply_id, tx).await,
            KvPayload::WriteOk(p) => p.handle_tso(reply_id, tx).await,
            KvPayload::Cas(p) => p.handle_tso(reply_id, tx).await,
            KvPayload::CasOk(p) => p.handle_tso(reply_id, tx).await,
            KvPayload::Error(p) => p.handle_tso(reply_id, tx).await,
            KvPayload::Ts(p) => p.handle_tso(reply_id, tx).await,
            KvPayload::TsOk(p) => p.handle_tso(reply_id, tx).await,
        }
    }
}

pub(crate) trait HandleKvPayload {
    async fn handle_kv(self, reply_id: u64, tx: &Sender<KvMsg>) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        let _ = reply_id;
        let _ = tx;
        bail!("unexpected kv payload variant");
    }
}

pub(crate) trait HandleTsoPayload {
    async fn handle_tso(self, reply_id: u64, tx: &Sender<TsoMsg>) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        let _ = reply_id;
        let _ = tx;
        bail!("unexpected tso payload variant");
    }
}

impl HandleKvPayload for Read {}
impl HandleKvPayload for Write {}
impl HandleKvPayload for Cas {}
impl HandleKvPayload for Ts {}
impl HandleKvPayload for TsOk {}

impl HandleKvPayload for ReadOk {
    async fn handle_kv(self, reply_id: u64, tx: &Sender<KvMsg>) -> anyhow::Result<()> {
        tx.send(KvMsg::ReadResponse(KvMsgReadResponse {
            msg_id: reply_id,
            value: self.value,
        }))
        .await
        .context("failed to send read response")?;
        Ok(())
    }
}

impl HandleKvPayload for WriteOk {
    async fn handle_kv(self, reply_id: u64, tx: &Sender<KvMsg>) -> anyhow::Result<()> {
        tx.send(KvMsg::WriteResponse(KvMsgWriteResponse {
            msg_id: reply_id,
        }))
        .await
        .context("failed to send write response")?;
        Ok(())
    }
}

impl HandleKvPayload for CasOk {
    async fn handle_kv(self, reply_id: u64, tx: &Sender<KvMsg>) -> anyhow::Result<()> {
        tx.send(KvMsg::CmpAndSwpResponse(KvMsgCmpAndSwpResponse {
            msg_id: reply_id,
            swapped: true,
        }))
        .await
        .context("failed to send cas response")?;
        Ok(())
    }
}

impl HandleKvPayload for Error {
    async fn handle_kv(self, reply_id: u64, tx: &Sender<KvMsg>) -> anyhow::Result<()> {
        tx.send(KvMsg::ErrorResponse(KvMsgErrorResponse {
            msg_id: reply_id,
            code: self.code,
            text: self.text,
        }))
        .await
        .context("failed to send error response")?;
        Ok(())
    }
}

impl HandleTsoPayload for Read {}
impl HandleTsoPayload for ReadOk {}
impl HandleTsoPayload for Write {}
impl HandleTsoPayload for WriteOk {}
impl HandleTsoPayload for Cas {}
impl HandleTsoPayload for CasOk {}
impl HandleTsoPayload for Error {}
impl HandleTsoPayload for Ts {}

impl HandleTsoPayload for TsOk {
    async fn handle_tso(self, reply_id: u64, tx: &Sender<TsoMsg>) -> anyhow::Result<()> {
        tx.send(TsoMsg::TsOk(TsoMsgTsOk {
            msg_id: reply_id,
            ts: self.ts,
        }))
        .await
        .context("failed to send ts response")?;
        Ok(())
    }
}
