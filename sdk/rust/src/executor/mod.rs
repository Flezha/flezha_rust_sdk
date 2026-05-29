mod control_unit;

pub(crate) mod fast;
pub(crate) mod mailbox;
pub(crate) mod persistent_id;
pub(crate) mod work_stealing_pool;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use crossbeam::channel::Sender;

use crate::relay::instance::RelayInstance;

/// Process-global handle to the coordinator's message channel, installed
/// once by `fast::start`. Lets components outside the coordinator (e.g.
/// the partition-state sink on a broken-pipe flood) request a control
/// reconnect without threading the `Sender` through every layer.
static RECONNECT_TRIGGER: OnceLock<Mutex<Sender<ExecutorMessage>>> = OnceLock::new();

pub(crate) fn install_reconnect_trigger(otx: Sender<ExecutorMessage>) {
    let _ = RECONNECT_TRIGGER.set(Mutex::new(otx));
}

/// Ask the coordinator to drive a control-channel reconnect. No-op when
/// the trigger is not installed (e.g. unit tests). `reason` is one of the
/// canonical strings documented on [`ExecutorMessage::Reconnect`].
pub(crate) fn request_reconnect(reason: &'static str) {
    if let Some(lock) = RECONNECT_TRIGGER.get() {
        if let Ok(tx) = lock.lock() {
            let _ = tx.send(ExecutorMessage::Reconnect(reason));
        }
    }
}

/// Serializes every test that mutates the process-global executor env vars
/// (`FLEZHA_EXECUTOR_ID_PATH`, `FLEZHA_CONTROL_*`). These knobs are shared
/// across the whole crate, so the lock lives here (not in one child module)
/// and is poison-tolerant: a failed assertion inside one holder must not
/// cascade-fail the unrelated tests that merely share the lock.
#[cfg(test)]
static ENV_TEST_LOCK: Mutex<()> = Mutex::new(());

#[cfg(test)]
pub(crate) fn env_test_guard() -> std::sync::MutexGuard<'static, ()> {
    ENV_TEST_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Messages routed through the coordinator channel. Data-plane traffic
/// bypasses this channel entirely (per-Partition mailboxes are pushed
/// directly by the stream readers); only control-plane traffic flows
/// through here.
pub enum ExecutorMessage {
    HandleCommandAction(String, Vec<u8>, HashMap<String, String>),
    ReturnRelays,
    /// Drive a reconnect with a structured reason for the observer log
    /// and the `ExecutorReconnected` control frame the controller now
    /// expects on the first byte of the new stream. Reason is one of
    /// `control_stream_broken | read_error | write_error |
    /// keepalive_timeout | controller_requested | broken_pipe_flood`.
    Reconnect(&'static str),
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
