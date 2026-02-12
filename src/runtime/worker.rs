use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::runtime::sender::MessageSender;

#[async_trait]
pub trait Worker<P: Send + 'static>: Clone + Send + Sync + 'static {
    fn tick_interval(&self) -> Option<Duration> {
        None
    }

    async fn handle_tick<T: MessageSender<P>>(&self, _tx: T) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl<P> Worker<P> for () where P: Debug + Serialize + DeserializeOwned + Send + Sync + 'static {}
