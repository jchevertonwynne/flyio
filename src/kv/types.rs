use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use moka::sync::Cache;

use crate::runtime::ServiceSlot;

pub const ERROR_CODE_KEY_NOT_EXIST: u16 = 20;
pub const ERROR_CODE_PRECONDITION_FAILED: u16 = 22;

pub trait StoreName: Send + Sync + 'static {
    fn name() -> &'static str;
}

#[derive(Debug, Clone)]
pub struct SeqStore;
impl StoreName for SeqStore {
    fn name() -> &'static str {
        "seq-kv"
    }
}

#[derive(Debug, Clone)]
pub struct LinStore;
impl StoreName for LinStore {
    fn name() -> &'static str {
        "lin-kv"
    }
}

#[derive(Debug, Clone)]
pub struct LwwStore;
impl StoreName for LwwStore {
    fn name() -> &'static str {
        "lww-kv"
    }
}

#[derive(Debug)]
pub enum KvError {
    KeyDoesNotExist,
    PreconditionFailed,
    Other {
        code: u16,
        text: String,
    },
    Timeout {
        operation: &'static str,
        duration: Duration,
    },
    Internal(anyhow::Error),
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvError::KeyDoesNotExist => write!(f, "key does not exist"),
            KvError::PreconditionFailed => write!(f, "precondition failed"),
            KvError::Other { code, text } => write!(f, "kv error code {code}: {text}"),
            KvError::Timeout {
                operation,
                duration,
            } => {
                write!(f, "{operation} timed out after {duration:?}")
            }
            KvError::Internal(err) => write!(f, "kv internal error: {err}"),
        }
    }
}

impl std::error::Error for KvError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            KvError::Internal(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct MsgIDProvider {
    inner: Arc<AtomicU64>,
    route_hook: Option<RouteHook>,
}

impl fmt::Debug for MsgIDProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MsgIDProvider").finish()
    }
}

impl Default for MsgIDProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl MsgIDProvider {
    #[must_use]
    pub fn new() -> MsgIDProvider {
        MsgIDProvider {
            inner: Arc::new(AtomicU64::new(0)),
            route_hook: None,
        }
    }

    #[must_use]
    pub fn with_route_hook(
        &self,
        routes: Cache<u64, ServiceSlot>,
        slot: ServiceSlot,
    ) -> MsgIDProvider {
        MsgIDProvider {
            inner: Arc::clone(&self.inner),
            route_hook: Some(RouteHook::new(routes, slot)),
        }
    }

    #[must_use]
    pub fn id(&self) -> u64 {
        let id = self.inner.fetch_add(1, Ordering::SeqCst);
        if let Some(hook) = &self.route_hook {
            hook.register(id);
        }
        id
    }
}

#[derive(Clone)]
struct RouteHook {
    routes: Cache<u64, ServiceSlot>,
    slot: ServiceSlot,
}

impl RouteHook {
    fn new(routes: Cache<u64, ServiceSlot>, slot: ServiceSlot) -> Self {
        Self { routes, slot }
    }

    fn register(&self, msg_id: u64) {
        self.routes.insert(msg_id, self.slot);
    }
}
