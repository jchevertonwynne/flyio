use anyhow::anyhow;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc::Sender};

use crate::message::{Body, Message};

#[async_trait]
pub trait MessageSender<P>: Send + Sync + Clone + 'static {
    async fn send(&self, msg: Message<Body<P>>) -> anyhow::Result<()>;
}

pub struct ChannelSender<P> {
    inner: Sender<Message<Body<P>>>,
}

impl<P> Clone for ChannelSender<P> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<P> ChannelSender<P> {
    pub fn new(inner: Sender<Message<Body<P>>>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<P> MessageSender<P> for ChannelSender<P>
where
    P: Send + 'static,
{
    async fn send(&self, msg: Message<Body<P>>) -> anyhow::Result<()> {
        self.inner
            .send(msg)
            .await
            .map_err(|_| anyhow!("outbound channel closed"))
    }
}

#[derive(Default)]
pub struct CollectSender<P> {
    inner: Arc<Mutex<Vec<Message<Body<P>>>>>,
}

impl <P> Clone for CollectSender<P> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<P> CollectSender<P> {
    pub async fn drain(&self) -> Vec<Message<Body<P>>> {
        let mut guard = self.inner.lock().await;
        std::mem::take(&mut *guard)
    }
}

#[async_trait]
impl<P> MessageSender<P> for CollectSender<P>
where
    P: Send + 'static,
{
    async fn send(&self, msg: Message<Body<P>>) -> anyhow::Result<()> {
        let mut guard = self.inner.lock().await;
        guard.push(msg);
        Ok(())
    }
}
