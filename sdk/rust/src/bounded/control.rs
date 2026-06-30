//! Per-instance runtime control for running bounded operatives:
//! process-global, one entry per started bounded instance. Holds the
//! cancellation token (tripped by `BoundedStop`, a hard `RemOperative`, or a
//! `max_duration` elapse) and the inbox sender (the dispatch path pushes
//! post-start input frames here instead of dropping them). Mirrors
//! `bounded::registry`'s process-global pattern so the command handlers and
//! the wire-dispatch path reach a running instance without a handle threaded
//! through every layer.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use crossbeam::channel::Sender;
use lazy_static::lazy_static;

/// Live control surface for one running bounded instance. Cloned cheaply
/// (two `Arc`s); the driver owns the receiving end of `inbox_tx`.
#[derive(Clone)]
pub(crate) struct BoundedControl {
    /// Tripped to request cooperative cancellation. The driver's loop /
    /// `recv` observe it within ≤200 ms and unwind to the single teardown.
    pub cancel: Arc<AtomicBool>,
    /// Inbox feeder. The wire-dispatch path sends post-start input frames
    /// here; `BoundedCtx::recv` drains them.
    pub inbox_tx: Sender<Vec<u8>>,
}

lazy_static! {
    static ref CONTROL_REGISTRY: RwLock<HashMap<String, BoundedControl>> =
        RwLock::new(HashMap::new());
}

/// Register a running instance's control surface. Called once by the driver
/// at start, before the body runs.
pub(crate) fn register(instance_id: &str, control: BoundedControl) {
    CONTROL_REGISTRY
        .write()
        .unwrap()
        .insert(instance_id.to_string(), control);
}

/// Remove an instance's control surface at terminal teardown. Idempotent.
pub(crate) fn remove(instance_id: &str) {
    CONTROL_REGISTRY.write().unwrap().remove(instance_id);
}

/// Trip the cancellation token for a running instance. Returns `true` if the
/// instance was found (live), `false` otherwise. Used by the cooperative
/// `BoundedStop` handler and the hard `RemOperative` path.
pub(crate) fn cancel(instance_id: &str) -> bool {
    match CONTROL_REGISTRY.read().unwrap().get(instance_id) {
        Some(control) => {
            control.cancel.store(true, Ordering::Release);
            true
        }
        None => false,
    }
}

/// Route a post-start input frame to a running instance's inbox. Returns
/// `true` if delivered, `false` if the instance is not running / has no live
/// inbox (the frame is dropped by the caller). A full/closed inbox also
/// drops.
pub(crate) fn push_inbox(instance_id: &str, payload: Vec<u8>) -> bool {
    match CONTROL_REGISTRY.read().unwrap().get(instance_id) {
        Some(control) => control.inbox_tx.send(payload).is_ok(),
        None => false,
    }
}
