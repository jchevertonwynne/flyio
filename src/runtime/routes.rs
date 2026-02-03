use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::kv::MsgIDProvider;

#[derive(Clone)]
pub(crate) struct RouteRegistry {
    routes: Arc<Mutex<HashMap<u64, ServiceSlot>>>,
}

impl RouteRegistry {
    pub(crate) fn new() -> Self {
        Self {
            routes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn bind(&self, provider: &MsgIDProvider, slot: ServiceSlot) -> MsgIDProvider {
        let routes = Arc::clone(&self.routes);
        provider.with_route_hook(routes, slot)
    }

    pub(crate) fn take(&self, msg_id: u64) -> Option<ServiceSlot> {
        self.routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&msg_id)
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
