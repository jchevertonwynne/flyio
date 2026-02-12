use moka::notification::RemovalCause;
use moka::sync::Cache;
use tracing::warn;

use crate::kv::MsgIDProvider;

const ROUTE_CACHE_CAPACITY: u64 = 65_536;

#[derive(Clone)]
pub struct RouteRegistry {
    routes: Cache<u64, ServiceSlot>,
}

impl RouteRegistry {
    pub(crate) fn new() -> Self {
        let routes = Cache::builder()
            .max_capacity(ROUTE_CACHE_CAPACITY)
            .eviction_listener(|msg_id, slot, cause| {
                if matches!(cause, RemovalCause::Explicit | RemovalCause::Replaced) {
                    return;
                }
                warn!(
                    msg_id = *msg_id,
                    slot = ?slot,
                    cause = ?cause,
                    "service route discarded before reply"
                );
            })
            .build();
        Self { routes }
    }

    pub(crate) fn bind(&self, provider: &MsgIDProvider, slot: ServiceSlot) -> MsgIDProvider {
        provider.with_route_hook(self.routes.clone(), slot)
    }

    pub(crate) fn take(&self, msg_id: u64) -> Option<ServiceSlot> {
        self.routes.remove(&msg_id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ServiceSlot {
    depth: u8,
    path: [u8; ServiceSlot::MAX_DEPTH],
}

impl ServiceSlot {
    const MAX_DEPTH: usize = 8;

    pub(crate) fn root() -> Self {
        Self {
            depth: 0,
            path: [0; ServiceSlot::MAX_DEPTH],
        }
    }

    pub(crate) fn child(&self, idx: u8) -> Self {
        let mut next = *self;
        debug_assert!(
            (next.depth as usize) < Self::MAX_DEPTH,
            "service route depth overflow"
        );
        assert!(
            (next.depth as usize) != Self::MAX_DEPTH,
            "service route depth exceeded {}",
            Self::MAX_DEPTH
        );
        next.path[next.depth as usize] = idx;
        next.depth += 1;
        next
    }

    pub(crate) fn split(mut self) -> Option<(u8, ServiceSlot)> {
        if self.depth == 0 {
            return None;
        }
        let head = self.path[0];
        for i in 1..self.depth as usize {
            self.path[i - 1] = self.path[i];
        }
        self.depth -= 1;
        Some((head, self))
    }
}
