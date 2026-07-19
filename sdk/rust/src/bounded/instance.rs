//! Per-`AddOperative` runtime state for a bounded-kind operative: its live
//! control surface (cancellation token + inbox) plus, for a structured class,
//! the pre-built Segment slots and the crash-resume gate.
//!
//! Held as the [`Operative::Bounded`](crate::operative::registry::Operative)
//! variant in the shared [`OperativeRegistry`](crate::operative::registry::OperativeRegistry)
//! — mirroring `UnboundedInstance` and the Java `BoundedOperativeInstance`, so
//! there is ONE per-instance registry. The `BoundedStop` / `RemOperative` /
//! `max_duration` cancels, the wire-dispatch inbox, and the inbound
//! `RestoreInitState` handler all reach a running bounded operative through the
//! same registry entry — no bounded-specific process-global.

use std::collections::HashSet;
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

/// Lock a compensation-marker set, recovering from a poisoned lock (the sets
/// only hold shipped-back Segment identities — no invariant to protect).
fn marker_set(set: &Mutex<HashSet<Vec<u8>>>) -> std::sync::MutexGuard<'_, HashSet<Vec<u8>>> {
    set.lock().unwrap_or_else(|p| p.into_inner())
}

/// One post-start input delivered to a running bounded instance: the payload
/// plus the durable-inbox offset the controller assigned it
/// (`bounded-durable-suspension`). `offset` is `0` when the input was not
/// journaled — an opted-out class, whose inbox is memory-only as before.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboxItem {
    pub payload: Vec<u8>,
    pub offset: u64,
}

pub struct BoundedInstance {
    pub instance_id: Arc<str>,
    /// The class this instance was spawned from — attributes live instances
    /// to a factory during a `remove_bounded` check (unbounded instances
    /// carry it via their topology).
    pub factory_id: Arc<str>,
    /// Tripped by `BoundedStop` / `RemOperative` / `max_duration`; observed by
    /// the driver's loops + `recv` within ≤200 ms.
    pub cancel: Arc<AtomicBool>,
    /// Inbox feeder; the driver owns the matching `Receiver`.
    inbox_tx: Sender<InboxItem>,
    /// Pre-built Segments per chain stage (empty for run-sugar). Shared with the
    /// driver; the restore handler rebuilds a slot's state in place before the
    /// gate releases the driver to read it.
    pub stage_segments: Vec<Vec<Arc<SegmentSlot>>>,
    /// Number of Segment restores to wait for before the first Operation runs.
    /// `0` ⇒ a fresh spawn / no persisted state / run-sugar ⇒ run immediately.
    expected: usize,
    restored: Mutex<usize>,
    restored_cv: Condvar,
    /// Segment identities whose operation completed forward and may need undo,
    /// shipped back via `RestoreInitState.has_compensation_intent` on
    /// crash-resume (saga). Non-empty ⇒ the driver finishes the compensation
    /// instead of re-running the forward chain.
    compensation_intents: Mutex<HashSet<Vec<u8>>>,
    /// Segment identities whose compensation already ran before the crash
    /// (`RestoreInitState.has_compensation_done`) — skipped on resume.
    compensation_done: Mutex<HashSet<Vec<u8>>>,
    /// Committed op-journal state shipped back via `RestoreInitState.op_journal`
    /// on crash-resume (`bounded-op-journal`). `Some` ⇒ the driver skips the
    /// journaled operations and seeds the first uncommitted one from the
    /// handoff. Every restore frame of the bind carries the same state, so
    /// repeated notes are idempotent overwrites.
    op_journal: Mutex<Option<JournalResume>>,
}

/// Committed op-journal state for one crash-resume. `iter` is the iteration to
/// resume at — the one after the highest journaled `iter_committed`, so a
/// looping instance never re-runs a committed iteration (`bounded-durable-loop`);
/// `committed_op_idxs` are the operations already committed inside that
/// still-in-flight iteration, and `op_handoffs` carries what each of them
/// released, so every uncommitted child branch re-seeds from its own parent's
/// edge (`bounded-branching`). `handoff` is the previous iteration's terminal
/// batch, which re-seeds a handoff-chained loop. Both are channel-id-tagged
/// codec bytes, decoded by the producing channel's inter-op codec at seed time.
/// `committed_ops` carries every journaled forward op commit with its iteration,
/// which the driver folds into the compensation planner's per-iteration
/// `committed` input.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JournalResume {
    pub committed_op_idxs: Vec<u32>,
    pub iter: u32,
    pub handoff: Vec<(u32, Vec<u8>)>,
    pub op_handoffs: Vec<EdgeHandoff>,
    pub committed_ops: Vec<CommittedOp>,
    /// Inbox offset the last committed iteration consumed — the floor a
    /// rehydrated inbox-driven loop reads its next item above.
    pub consumed_offset: u64,
    /// Operations a rewind unwound that nothing has compensated yet. The driver
    /// undoes them through the ordinary compensation planner before resuming
    /// forward at `iter` (which the journal has already folded back to the
    /// rewound-to iteration). Empty = no rewind pending.
    pub rewind_undo: Vec<CommittedOp>,
}

/// What one committed operation of the in-flight iteration released, so its
/// uncommitted children re-seed from exactly their own parent.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EdgeHandoff {
    pub op_idx: u32,
    pub handoff: Vec<(u32, Vec<u8>)>,
}

/// One journaled forward op commit, shipped back on resume.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedOp {
    pub op_idx: u32,
    pub iter: u32,
    pub segments_ran: Vec<Vec<u8>>,
}

impl BoundedInstance {
    /// Build the instance and return the driver's inbox `Receiver`. `expected`
    /// is the crash-resume restore count (0 for fresh / run-sugar / opted-out).
    pub fn new(
        instance_id: Arc<str>,
        factory_id: Arc<str>,
        stage_segments: Vec<Vec<Arc<SegmentSlot>>>,
        expected: usize,
    ) -> (Arc<Self>, Receiver<InboxItem>) {
        let (inbox_tx, inbox_rx) = unbounded();
        let instance = Arc::new(Self {
            instance_id,
            factory_id,
            cancel: Arc::new(AtomicBool::new(false)),
            inbox_tx,
            stage_segments,
            expected,
            restored: Mutex::new(0),
            restored_cv: Condvar::new(),
            compensation_intents: Mutex::new(HashSet::new()),
            compensation_done: Mutex::new(HashSet::new()),
            op_journal: Mutex::new(None),
        });
        (instance, inbox_rx)
    }

    /// Record the shipped-back op-journal state (crash-resume). Overwrites —
    /// every restore frame of one bind carries the same snapshot.
    pub fn note_op_journal(&self, resume: JournalResume) {
        *self.op_journal.lock().unwrap_or_else(|p| p.into_inner()) = Some(resume);
    }

    /// The shipped-back op-journal state, if any. Cloned so the driver reads
    /// it without holding the lock across the chain run.
    pub fn op_journal_resume(&self) -> Option<JournalResume> {
        self.op_journal
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Whether any Segment's compensation-done marker was shipped back — the
    /// signal that an undo pass was already in flight before the crash, so
    /// compensation resume outranks journal-gated forward resume.
    pub fn any_compensation_done(&self) -> bool {
        !marker_set(&self.compensation_done).is_empty()
    }

    /// Record the shipped-back saga compensation markers for a Segment (crash-
    /// resume): `intent` ⇒ its operation completed forward and may need undo;
    /// `done` ⇒ its compensation already ran (skip it on resume).
    pub fn note_compensation_markers(&self, segment_identity: &[u8], intent: bool, done: bool) {
        if intent {
            marker_set(&self.compensation_intents).insert(segment_identity.to_vec());
        }
        if done {
            marker_set(&self.compensation_done).insert(segment_identity.to_vec());
        }
    }

    /// Whether this Segment completed forward and needs undo (drives which hooks
    /// a crash-resume runs).
    pub fn has_compensation_intent(&self, segment_identity: &[u8]) -> bool {
        marker_set(&self.compensation_intents).contains(segment_identity)
    }

    /// Whether this Segment's compensation already ran (skip on resume).
    pub fn has_compensation_done(&self, segment_identity: &[u8]) -> bool {
        marker_set(&self.compensation_done).contains(segment_identity)
    }

    /// Trip the cancellation token (cooperative `BoundedStop`, hard
    /// `RemOperative`, or `max_duration` elapse).
    pub fn request_cancel(&self) {
        self.cancel.store(true, Ordering::Release);
    }

    /// Route a post-start input frame to the driver's inbox, carrying the
    /// durable-inbox offset the controller assigned it (`0` when unjournaled).
    /// `false` if the channel is closed (the driver has finished).
    pub fn push_inbox(&self, payload: Vec<u8>, offset: u64) -> bool {
        self.inbox_tx.send(InboxItem { payload, offset }).is_ok()
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
        BoundedInstance::new(
            Arc::from("gate-test"),
            Arc::from("gate-factory"),
            Vec::new(),
            expected,
        )
        .0
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
