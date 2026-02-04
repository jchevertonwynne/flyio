use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc::Sender;

use crate::message::{Body, Message};

#[async_trait]
pub trait Worker<P: Send + 'static>: Clone + Send + Sync + 'static {
    fn tick_interval(&self) -> Option<Duration> {
        None
    }

    async fn handle_tick(&self, _tx: Sender<Message<Body<P>>>) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl<P> Worker<P> for () where P: Debug + Serialize + DeserializeOwned + Send + Sync + 'static {}
