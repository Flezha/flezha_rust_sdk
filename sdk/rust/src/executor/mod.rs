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
    /// Tick from the keepalive timer: write a control-channel ping on the
    /// current control stream. Routed through the coordinator so control
    /// writes stay single-threaded (no interleave with command replies).
    SendControlKeepalive,
}

pub enum ExecutorStatus {
    Starting,
    Running,
    Stopped,
}

// reason: ReturnRelays carries the same public shape returned by Flezha::get_relays()
#[allow(clippy::type_complexity)]
pub enum ClientResponseMessage {
    ExecutorIdConfirmation,
    /// Snapshot of live Relay instances. Returned to `Flezha::get_relays()`
    /// callers — the public-API shape is preserved for backward
    /// compatibility, but each entry is now an `Arc<RelayInstance>` rather
    /// than `Arc<Mutex<RelayBox>>`.
    ReturnRelays(Arc<RwLock<HashMap<String, Arc<Mutex<Arc<RelayInstance>>>>>>),
}

pub use fast::{start, ExecutorNode};
