use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc::Sender;

use crate::message::{Body, Message};

#[async_trait]
pub trait Worker: Clone + Send + Sync + 'static {
    type Payload: Debug + Serialize + DeserializeOwned + Send + 'static;

    fn tick_interval(&self) -> Option<Duration> {
        None
    }

    async fn handle_tick(&self, _tx: Sender<Message<Body<Self::Payload>>>) -> anyhow::Result<()> {
        Ok(())
    }
}

pub struct NoWorker<P>(std::marker::PhantomData<P>);

impl<P> Default for NoWorker<P> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

// Clone is needed if Node is Clone and contains Worker, but Worker trait doesn't require Clone yet.
// However Node is Clone. So usually fields are Clone.
impl<P> Clone for NoWorker<P> {
    fn clone(&self) -> Self {
        Self(std::marker::PhantomData)
    }
}

#[async_trait]
impl<P> Worker for NoWorker<P>
where
    P: Debug + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Payload = P;
}
