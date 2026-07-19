//! Inbound `RestoreInitState`: controller → SDK warm restart of a Segment.
//!
//! The controller ships the persisted snapshot + WAL change-log for a
//! Segment after a runner restart / rebind. This handler locates the
//! live `SegmentSlot` whose wire identity matches `segment_id` and
//! rebuilds its (type-erased) state in place via the dispatcher's
//! `restore_state` — so a operative that summed to N comes back at N instead of
//! starting empty. It then re-arms the segment's `change_seq` to
//! `terminal_seq + 1` so the next produced frame is contiguous with the
//! persisted history and the controller does not realign (truncate) the WAL.
//!
//! When no live slot matches yet (the operative has not been re-added), it falls
//! back to validating the change-log and acking the terminal seq.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use prost::Message;

use super::CommandAction;
use crate::pb::{RestoreInitState, RestoreInitStateResponse};
use crate::runner::RunnerNode;
use crate::segment::restore::iter_change_log_records;
use crate::segment::SegmentSlot;
use crate::util::LockRecover;

impl CommandAction for RestoreInitState {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let terminal_seq = if let Some(slot) = find_segment_slot(runner, &self.segment_id) {
            restore_into_slot(&slot, self, errors)
        } else if let Some((instance, slot)) = runner.find_bounded_segment(&self.segment_id) {
            // Durable bounded Segment: rebuild its committed state in place and
            // release one count on the driver's crash-resume gate. The frame
            // landed regardless of apply success, so the gate never stalls on a
            // corrupt record.
            let t = restore_into_slot(&slot, self, errors);
            // Saga crash-resume (§4.3): carry the per-Segment compensation markers
            // the controller persisted so the driver finishes an in-flight undo
            // (intent) and skips segments already undone (done).
            instance.note_compensation_markers(
                &self.segment_id,
                self.has_compensation_intent,
                self.has_compensation_done,
            );
            // Op-journal crash-resume (bounded-op-journal): carry the
            // committed op state so the driver skips journaled operations
            // and seeds the next one from the journaled handoff.
            if let Some(journal) = &self.op_journal {
                instance.note_op_journal(crate::bounded::instance::JournalResume {
                    committed_op_idxs: journal.committed_op_idxs.clone(),
                    iter: journal.iter,
                    handoff: handoff_items(&journal.handoff),
                    op_handoffs: journal
                        .op_handoffs
                        .iter()
                        .map(|edge| crate::bounded::instance::EdgeHandoff {
                            op_idx: edge.op_idx,
                            handoff: handoff_items(&edge.handoff),
                        })
                        .collect(),
                    committed_ops: journal.committed_ops.iter().map(committed_op).collect(),
                    consumed_offset: journal.consumed_offset,
                    rewind_undo: journal.rewind_undo.iter().map(committed_op).collect(),
                });
            }
            instance.note_restored();
            t
        } else {
            // No live slot — validate the change-log and ack the terminal seq
            // (the controller still advances its handshake).
            let mut terminal = 0u64;
            for record in iter_change_log_records(&self.change_log_bytes) {
                match record {
                    Ok(r) => terminal = terminal.max(r.seq),
                    Err(e) => {
                        errors.push(format!("change_log decode error: {e}"));
                        return ("restore_init_state".to_string(), Vec::new());
                    }
                }
            }
            terminal
        };

        let response = RestoreInitStateResponse {
            segment_id: self.segment_id.clone(),
            terminal_seq,
        };

        ("restore_init_state".to_string(), response.encode_to_vec())
    }
}

/// Rebuild one live Segment's typed state from the shipped snapshot + change-log
/// and re-arm its `change_seq` so post-restore appends stay contiguous (the
/// controller then skips epoch realign). Shared by the unbounded and bounded
/// restore paths. Returns the terminal WAL seq (0 on apply failure).
fn restore_into_slot(
    slot: &Arc<SegmentSlot>,
    cmd: &RestoreInitState,
    errors: &mut Vec<String>,
) -> u64 {
    let mut guard = match slot.state.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    match slot.dispatcher.restore_state(
        &mut guard,
        &cmd.snapshot_bytes,
        &cmd.change_log_bytes,
        slot.operation_key.as_deref(),
    ) {
        Ok(t) => {
            drop(guard);
            slot.change_seq.store(t + 1, Ordering::Release);

            // A restore re-seats the Segment on a different past — the crash's,
            // or (for a correction) a deliberately rewound one. Anything the
            // old past left in flight belongs to a state that no longer exists:
            // the mailbox holds inputs the controller is about to re-forward
            // from the durable log anyway (every tier input is journaled before
            // it is forwarded, so the log is a superset of the mailbox), and a
            // half-filled replay buffer belongs to a slice that was abandoned.
            // Keeping either would double-apply them, and — worse — apply them
            // *after* the replayed slice in arrival order, reintroducing the
            // exact mis-ordering a correction exists to repair. An open
            // checkpoint interval is the same story: its changes were never
            // acknowledged, so the restored state does not contain them and
            // the re-forwarded inputs will produce them again.
            slot.mailbox.clear();
            slot.replay_buffer.lock_recover().clear();
            slot.replay_depth.store(0, Ordering::Relaxed);
            slot.replay_open.store(false, Ordering::Release);
            slot.flush_buffer.lock_recover().discard_staged();

            let cursors = restore_cursors(&cmd.change_log_bytes);
            // Effectively-once: remember the highest input-log offset the
            // restored effect records durably consumed, so the crash-replay
            // re-forward skips already-applied inputs (`drain_segment`).
            if let Some(floor) = cursors.consumed_offset {
                slot.consumed_floor.store(floor as i64, Ordering::Release);
            }
            // Correction: rebuild the event-time high-water, so a resumed
            // Segment neither mistakes its own history for a flood of late
            // inputs (high-water too low) nor misses a genuinely late one.
            slot.event_time_high_water.store(
                cursors.event_time_high_water.map_or(-1, |t| t as i64),
                Ordering::Release,
            );
            t
        }
        Err(e) => {
            errors.push(format!("restore_init_state: apply failed: {e}"));
            0
        }
    }
}

/// Per-Segment cursors folded out of a restore payload's change log.
#[derive(Debug, Default, PartialEq, Eq)]
struct RestoreCursors {
    /// Highest `consumed_offset` across the effect records; `None` when the log
    /// carries none (plain change records / pre-tier history).
    consumed_offset: Option<u64>,
    /// Highest `event_time`. The change log holds every record above the
    /// snapshot, and compaction may not truncate inside the correction window
    /// (`compact_after_age_ms >= correction_window`), so the retained records
    /// always cover the window — and an event older than the window is
    /// beyond-window by definition, correctable by nobody.
    event_time_high_water: Option<u64>,
}

/// Fold the change-log's effect records (`format_id = 2` at header byte 9) into
/// the cursors a restored Segment resumes from.
fn restore_cursors(change_log_bytes: &[u8]) -> RestoreCursors {
    let mut cursors = RestoreCursors::default();
    for record in iter_change_log_records(change_log_bytes) {
        let Ok(record) = record else { break };
        if record.header.get(9) == Some(&crate::segment::header::FORMAT_ID_EFFECT_RECORD) {
            if let Ok(body) = postcard::from_bytes::<crate::segment::flusher::EffectRecordBody>(
                record.change_bytes,
            ) {
                cursors.consumed_offset = cursors.consumed_offset.max(body.consumed_offset);
                cursors.event_time_high_water =
                    cursors.event_time_high_water.max(Some(body.event_time));
            }
        }
    }
    cursors
}

fn committed_op(op: &crate::pb::OpCommittedOp) -> crate::bounded::instance::CommittedOp {
    crate::bounded::instance::CommittedOp {
        op_idx: op.op_idx,
        iter: op.iter,
        segments_ran: op.segments_ran.clone(),
    }
}

/// A journaled handoff batch as the driver's seed form: channel-id-tagged bytes.
fn handoff_items(items: &[crate::pb::OpHandoffItem]) -> Vec<(u32, Vec<u8>)> {
    items
        .iter()
        .map(|item| (item.channel_id, item.payload.clone()))
        .collect()
}

/// Scan every live operative's segments for the slot whose wire identity equals
/// `segment_id`. The SDK encodes `segment_identity` with the same framing
/// the controller uses for `segment_id` (`[rc_len][rc][sh_len][sh][pk_len][pk]`),
/// so a byte-equal match is exact. Returns a cloned `Arc` so the operative-registry
/// locks are released before the (potentially slow) state rebuild.
fn find_segment_slot(runner: &RunnerNode, segment_id: &[u8]) -> Option<Arc<SegmentSlot>> {
    for instance in runner.get_operatives().values() {
        for operation_segments in instance.segments.iter() {
            for slot in operation_segments.iter() {
                if slot.segment_identity.as_slice() == segment_id {
                    return Some(Arc::clone(slot));
                }
            }
        }
    }

    None
}
