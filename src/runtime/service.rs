use anyhow::bail;
use std::future::Future;

use crate::kv::{KvClient, KvPayload, MsgIDProvider, StoreName, TsoClient};
use crate::message::{Body, Message};
use crate::runtime::sender::MessageSender;

use super::routes::{RouteRegistry, ServiceSlot};

pub trait Service: Sized + Send + Sync + Clone + 'static {
    fn init<T: MessageSender<KvPayload>>(
        id_provider: MsgIDProvider,
        node_id: String,
        tx: T,
        routes: RouteRegistry,
        slot: ServiceSlot,
    ) -> Self;

    fn process(
        &self,
        msg: Message<Body<KvPayload>>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    fn dispatch(
        &self,
        _slot: ServiceSlot,
        msg: Message<Body<KvPayload>>,
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        self.process(msg)
    }

    fn stop(&self) -> impl Future<Output = ()> + Send;
}

impl Service for () {
    fn init<T: MessageSender<KvPayload>>(
        _id_provider: MsgIDProvider,
        _node_id: String,
        _tx: T,
        _routes: RouteRegistry,
        _slot: ServiceSlot,
    ) -> Self {
    }

    async fn process(&self, _msg: Message<Body<KvPayload>>) -> anyhow::Result<()> {
        Ok(())
    }

    async fn stop(&self) {}
}

impl<S: StoreName + Send + Sync + Clone + 'static> Service for KvClient<S> {
    fn init<T: MessageSender<KvPayload>>(
        id_provider: MsgIDProvider,
        node_id: String,
        tx: T,
        routes: RouteRegistry,
        slot: ServiceSlot,
    ) -> Self {
        let id_provider = routes.bind(&id_provider, slot);
        KvClient::<S>::new(id_provider, node_id, tx)
    }

    async fn process(&self, msg: Message<Body<KvPayload>>) -> anyhow::Result<()> {
        KvClient::<S>::process(self, msg).await
    }

    async fn stop(&self) {
        KvClient::<S>::stop(self).await;
    }
}

impl Service for TsoClient {
    fn init<T: MessageSender<KvPayload>>(
        id_provider: MsgIDProvider,
        node_id: String,
        tx: T,
        routes: RouteRegistry,
        slot: ServiceSlot,
    ) -> Self {
        let id_provider = routes.bind(&id_provider, slot);
        TsoClient::new(id_provider, node_id, tx)
    }

    async fn process(&self, msg: Message<Body<KvPayload>>) -> anyhow::Result<()> {
        TsoClient::process(self, msg).await
    }

    async fn stop(&self) {
        TsoClient::stop(self).await;
    }
}

macro_rules! tuple_dispatch_chain {
    ($idx_val:expr, $rest:expr, $msg:ident, $counter:expr, $head:expr $(, $tail:expr)*) => {
        if $idx_val == $counter {
            $head
                .dispatch($rest, $msg.take().expect("tuple message already dispatched"))
                .await
        } else {
            tuple_dispatch_chain!($idx_val, $rest, $msg, $counter + 1, $( $tail ),*)
        }
    };
    ($idx_val:expr, $rest:expr, $msg:ident, $counter:expr,) => {
        bail!("unknown service slot index {idx}", idx = $idx_val)
    };
}

macro_rules! impl_service_for_tuple {
    ( $( $name:ident ),+ ) => {
        impl<$( $name: Service ),+> Service for ( $( $name, )+ ) {
            fn init<T: MessageSender<KvPayload>>(
                id_provider: MsgIDProvider,
                node_id: String,
                tx: T,
                routes: RouteRegistry,
                slot: ServiceSlot,
            ) -> Self {
                let mut slot_idx: u8 = 0;
                (
                    $(
                        {
                            let current_idx = slot_idx;
                            slot_idx += 1;
                            _ = slot_idx; // silence warnings for final tuple value
                            let child_slot = slot.child(current_idx);
                            $name::init(
                                id_provider.clone(),
                                node_id.clone(),
                                tx.clone(),
                                routes.clone(),
                                child_slot,
                            )
                        },
                    )+
                )
            }

            async fn process(&self, msg: Message<Body<KvPayload>>) -> anyhow::Result<()> {
                bail!("received unrouted service message: {msg:?}")
            }

            async fn dispatch(&self, slot: ServiceSlot, msg: Message<Body<KvPayload>>)
                -> anyhow::Result<()>
            {
                let Some((idx, rest)) = slot.split() else {
                    bail!("missing child slot while dispatching tuple service");
                };
                let ( $( casey::lower!($name), )+ ) = self;
                let mut msg = Some(msg);
                tuple_dispatch_chain!(
                    idx,
                    rest,
                    msg,
                    0,
                    $( casey::lower!($name) ),+
                )
            }

            async fn stop(&self) {
                let ( $( casey::lower!($name), )+ ) = self;
                tokio::join!(
                    $(
                        casey::lower!($name).stop(),
                    )+
                );
            }
        }
    };
}

impl_service_for_tuple!(A);
impl_service_for_tuple!(A, B);
impl_service_for_tuple!(A, B, C);
impl_service_for_tuple!(A, B, C, D);
