mod control_unit;

pub(crate) mod fast;
pub(crate) mod mailbox;
pub(crate) mod work_stealing_pool;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::relay::instance::RelayInstance;

/// Messages routed through the coordinator channel. Data-plane traffic
/// bypasses this channel entirely (per-Partition mailboxes are pushed
/// directly by the stream readers); only control-plane traffic flows
/// through here.
pub enum ExecutorMessage {
    HandleCommandAction(String, Vec<u8>, HashMap<String, String>),
    ReturnRelays,
    Reconnect,
}

pub enum ExecutorStatus {
    Starting,
    Running,
    Stopped,
}

pub enum ClientResponseMessage {
    ExecutorIdConfirmation,
    /// Snapshot of live Relay instances. Returned to `Flezha::get_relays()`
    /// callers — the public-API shape is preserved for backward
    /// compatibility, but each entry is now an `Arc<RelayInstance>` rather
    /// than `Arc<Mutex<RelayBox>>`.
    ReturnRelays(Arc<RwLock<HashMap<String, Arc<Mutex<Arc<RelayInstance>>>>>>),
}

pub use fast::{start, ExecutorNode};
