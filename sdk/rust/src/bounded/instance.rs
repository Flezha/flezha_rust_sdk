//! Per-`AddOperative` runtime state for a bounded-kind operative: its live
//! control surface (cancellation token + inbox) plus, for a structured class,
//! the pre-built Segment slots and the crash-resume gate.
//!
//! Held as the [`Operative::Bounded`](crate::operative::registry::Operative)
//! variant in the shared [`OperativeRegistry`](crate::operative::registry::OperativeRegistry)
//! — mirroring `ContinuousInstance` and the Java `BoundedOperativeInstance`, so
//! there is ONE per-instance registry. The `BoundedStop` / `RemOperative` /
//! `max_duration` cancels, the wire-dispatch inbox, and the inbound
//! `RestoreInitState` handler all reach a running bounded operative through the
//! same registry entry — no bounded-specific process-global.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crossbeam::channel::{unbounded, Receiver, Sender};

use crate::segment::SegmentSlot;

/// Safety-net cap on the crash-resume wait. The `expected` count is the
/// authoritative release signal; this only prevents a permanent hold if a
/// restore never lands (e.g. a controller/SDK persisted-Segment mismatch),
/// degrading to a best-effort re-run rather than a hung operative.
const RESUME_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
/// Re-check cadence for the resume wait so a cooperative cancel is observed
/// promptly (mirrors the reaper's `sleep_responsive`).
const RESUME_WAIT_CHUNK: Duration = Duration::from_millis(200);

pub struct BoundedInstance {
    pub instance_id: Arc<str>,
    /// Tripped by `BoundedStop` / `RemOperative` / `max_duration`; observed by
    /// the driver's loops + `recv` within ≤200 ms.
    pub cancel: Arc<AtomicBool>,
    /// Inbox feeder; the driver owns the matching `Receiver`.
    inbox_tx: Sender<Vec<u8>>,
    /// Pre-built Segments per chain stage (empty for run-sugar). Shared with the
    /// driver; the restore handler rebuilds a slot's state in place before the
    /// gate releases the driver to read it.
    pub stage_segments: Vec<Vec<Arc<SegmentSlot>>>,
    /// Number of Segment restores to wait for before the first Operation runs.
    /// `0` ⇒ a fresh spawn / no persisted state / run-sugar ⇒ run immediately.
    expected: usize,
    restored: Mutex<usize>,
    restored_cv: Condvar,
}

impl BoundedInstance {
    /// Build the instance and return the driver's inbox `Receiver`. `expected`
    /// is the crash-resume restore count (0 for fresh / run-sugar / opted-out).
    pub fn new(
        instance_id: Arc<str>,
        stage_segments: Vec<Vec<Arc<SegmentSlot>>>,
        expected: usize,
    ) -> (Arc<Self>, Receiver<Vec<u8>>) {
        let (inbox_tx, inbox_rx) = unbounded();
        let instance = Arc::new(Self {
            instance_id,
            cancel: Arc::new(AtomicBool::new(false)),
            inbox_tx,
            stage_segments,
            expected,
            restored: Mutex::new(0),
            restored_cv: Condvar::new(),
        });
        (instance, inbox_rx)
    }

    /// Trip the cancellation token (cooperative `BoundedStop`, hard
    /// `RemOperative`, or `max_duration` elapse).
    pub fn request_cancel(&self) {
        self.cancel.store(true, Ordering::Release);
    }

    /// Route a post-start input frame to the driver's inbox. `false` if the
    /// channel is closed (the driver has finished).
    pub fn push_inbox(&self, payload: Vec<u8>) -> bool {
        self.inbox_tx.send(payload).is_ok()
    }

    /// Locate one of this instance's Segment slots by wire identity, for the
    /// inbound `RestoreInitState` handler. `None` if it isn't ours.
    pub fn segment_by_identity(&self, segment_id: &[u8]) -> Option<Arc<SegmentSlot>> {
        self.stage_segments
            .iter()
            .flatten()
            .find(|slot| slot.segment_identity.as_slice() == segment_id)
            .map(Arc::clone)
    }

    /// Record one landed Segment restore and wake a driver waiting on the gate.
    pub fn note_restored(&self) {
        let mut n = self.restored.lock().unwrap_or_else(|p| p.into_inner());
        *n += 1;
        self.restored_cv.notify_all();
    }

    /// Hold the calling driver until every expected Segment restore has landed,
    /// a cooperative cancel trips, or the safety-net timeout elapses. A no-op
    /// when `expected == 0` (fresh spawn / opted-out / run-sugar).
    pub fn wait_for_restore(&self) {
        if self.expected == 0 {
            return;
        }
        let deadline = Instant::now() + RESUME_WAIT_TIMEOUT;
        let mut n = self.restored.lock().unwrap_or_else(|p| p.into_inner());
        while *n < self.expected {
            if self.cancel.load(Ordering::Acquire) {
                return;
            }
            let now = Instant::now();
            if now >= deadline {
                tracing::warn!(
                    target: "flezha::bounded",
                    expected = self.expected,
                    restored = *n,
                    "bounded crash-resume timed out waiting for Segment restores; \
                     proceeding with partially-restored state"
                );
                return;
            }
            let wait = RESUME_WAIT_CHUNK.min(deadline - now);
            let (guard, _) = self
                .restored_cv
                .wait_timeout(n, wait)
                .unwrap_or_else(|p| p.into_inner());
            n = guard;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An instance with no Segments — enough to exercise the resume-count logic
    /// (which never touches `stage_segments`) without building real slots.
    fn gate(expected: usize) -> Arc<BoundedInstance> {
        BoundedInstance::new(Arc::from("gate-test"), Vec::new(), expected).0
    }

    #[test]
    fn zero_expected_never_blocks() {
        gate(0).wait_for_restore();
    }

    #[test]
    fn gate_releases_once_expected_restores_land() {
        // Recording `expected` restores up front means the next wait returns
        // immediately — the count is the authoritative release signal.
        let g = gate(2);
        g.note_restored();
        g.note_restored();
        g.wait_for_restore();
    }

    #[test]
    fn cancel_short_circuits_the_gate() {
        let g = gate(5);
        g.request_cancel();
        g.wait_for_restore();
    }

    #[test]
    fn note_restored_wakes_a_blocked_waiter() {
        // A driver already parked on the gate is released as soon as the last
        // expected restore lands — the cross-thread wake path.
        let g = gate(1);
        let (started_tx, started_rx) = unbounded::<()>();
        let waiter = {
            let g = Arc::clone(&g);
            std::thread::spawn(move || {
                let _ = started_tx.send(());
                g.wait_for_restore();
            })
        };
        let _ = started_rx.recv();
        g.note_restored();
        waiter.join().expect("waiter joins after its restore lands");
    }
}
