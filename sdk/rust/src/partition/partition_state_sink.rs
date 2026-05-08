//! Global send-frame sink for the per-Partition flusher.
//!
//! At SDK boot (after the secondary streams are connected), the streams
//! connect path calls [`install_global_sink`] with a closure that wraps
//! one of the secondary `FlezhaStream`s. The flusher fetches the
//! installed sink at registration time (or every drain pass via the
//! atomic-pointer load) and uses it to ship `partition_state_change` /
//! `partition_state_snapshot` frames to the controller.
//!
//! When no sink is installed (e.g. unit tests not touching the
//! transport), [`current_send_frame_fn`] returns
//! [`crate::partition::flusher::fallback_send_frame`] which fails loud
//! enough that the flusher decrements its seq and retries on the next
//! pass.

use std::sync::{Arc, RwLock};

use crate::partition::flusher::{fallback_send_frame, SendError, SendFrameFn};

static SINK: RwLock<Option<SendFrameFn>> = RwLock::new(None);

/// Install (or replace) the global send-frame fn. Idempotent — calling
/// twice replaces the prior sink. The SDK calls this once after the
/// secondary streams are up.
pub fn install_global_sink(send_fn: SendFrameFn) {
    let mut guard = SINK.write().expect("partition_state_sink lock poisoned");
    *guard = Some(send_fn);
}

/// Drop the global sink. Called when the SDK is tearing down secondary
/// streams (e.g. graceful shutdown). Subsequent flusher passes will hit
/// [`fallback_send_frame`].
pub fn uninstall_global_sink() {
    let mut guard = SINK.write().expect("partition_state_sink lock poisoned");
    *guard = None;
}

/// Returns the currently-installed send-frame fn, or the fallback when
/// none is installed.
pub fn current_send_frame_fn() -> SendFrameFn {
    let guard = SINK.read().expect("partition_state_sink lock poisoned");
    match guard.as_ref() {
        Some(f) => Arc::clone(f),
        None => Arc::new(fallback_send_frame as fn(Vec<u8>) -> Result<(), SendError>),
    }
}
