//! Inbound `RestoreInitState`: controller → SDK warm restart of an Entity.
//!
//! The controller ships the persisted snapshot + WAL change-log for a
//! Entity after a runner restart / rebind. This handler locates the
//! live `EntitySlot` whose wire identity matches `entity_id` and
//! rebuilds its (type-erased) state in place via the dispatcher's
//! `restore_state` — so a operative that summed to N comes back at N instead of
//! starting empty. It then re-arms the entity's `change_seq` to
//! `terminal_seq + 1` so the next produced frame is contiguous with the
//! persisted history and the controller does not realign (truncate) the WAL.
//!
//! When no live slot matches yet (the operative has not been re-added), it falls
//! back to validating the change-log and acking the terminal seq.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use prost::Message;

use super::CommandAction;
use crate::entity::restore::iter_change_log_records;
use crate::entity::EntitySlot;
use crate::pb::{RestoreInitState, RestoreInitStateResponse};
use crate::runner::RunnerNode;
use crate::util::LockRecover;

impl CommandAction for RestoreInitState {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let terminal_seq = if let Some(slot) = find_entity_slot(runner, &self.entity_id) {
            restore_into_slot(&slot, self, errors)
        } else if let Some((instance, slot)) = runner.find_bounded_entity(&self.entity_id) {
            // Durable bounded Entity: rebuild its committed state in place and
            // release one count on the driver's crash-resume gate. The frame
            // landed regardless of apply success, so the gate never stalls on a
            // corrupt record.
            let t = restore_into_slot(&slot, self, errors);
            // Saga crash-resume (§4.3): carry the per-Entity compensation markers
            // the controller persisted so the driver finishes an in-flight undo
            // (intent) and skips entities already undone (done).
            instance.note_compensation_markers(
                &self.entity_id,
                self.has_compensation_intent,
                self.has_compensation_done,
            );
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
            entity_id: self.entity_id.clone(),
            terminal_seq,
        };

        ("restore_init_state".to_string(), response.encode_to_vec())
    }
}

/// Rebuild one live Entity's typed state from the shipped snapshot + change-log
/// and re-arm its `change_seq` so post-restore appends stay contiguous (the
/// controller then skips epoch realign). Shared by the unbounded and bounded
/// restore paths. Returns the terminal WAL seq (0 on apply failure).
fn restore_into_slot(
    slot: &Arc<EntitySlot>,
    cmd: &RestoreInitState,
    errors: &mut Vec<String>,
) -> u64 {
    let mut guard = match slot.state.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let mut timers = slot.timers.lock_recover();
    match slot.dispatcher.restore_state(
        &mut guard,
        &mut timers,
        &cmd.snapshot_bytes,
        &cmd.change_log_bytes,
        slot.entity_key.as_deref(),
    ) {
        Ok(t) => {
            drop(timers);
            drop(guard);
            slot.change_seq.store(t + 1, Ordering::Release);

            // A restore re-seats the Entity on a different past — the crash's,
            // or (for a correction) a deliberately rewound one. An open
            // checkpoint interval's changes were never acknowledged, so the
            // restored state does not contain them and the re-forwarded inputs
            // produce them again; a half-filled replay buffer belongs to a
            // slice that was abandoned. Both go, on every class.
            slot.discard_staged_interval();
            slot.discard_replay_slice();
            // The mailbox goes only where the controller re-forwards it; the
            // slot owns that rule.
            let discarded = slot.discard_queued_input_on_restore();
            if discarded > 0 {
                eprintln!(
                    "[flezha] restore discarded {discarded} queued input(s) for entity {} \
                     — the controller re-forwards them from the durable input log",
                    slot.dispatcher.entity_label()
                );
            }

            let cursors = restore_cursors(&cmd.change_log_bytes);
            // Effectively-once: remember the highest input-log offset the
            // restored effect records durably consumed, so the crash-replay
            // re-forward skips already-applied inputs (`drain_entity`).
            if let Some(floor) = cursors.consumed_offset {
                slot.consumed_floor.store(floor as i64, Ordering::Release);
            }
            // Correction: rebuild the event-time high-water, so a resumed
            // Entity neither mistakes its own history for a flood of late
            // inputs (high-water too low) nor misses a genuinely late one.
            slot.event_time_high_water.store(
                cursors.event_time_high_water.map_or(-1, |t| t as i64),
                Ordering::Release,
            );
            // Pending timers came back with the state; the processing-time ones
            // need an entry on the wheel again. Anything already past its
            // deadline fires on the next pass, ascending. Event-time timers
            // need nothing — the restored high-water is their trigger.
            crate::entity::timer::rearm_restored(slot);
            t
        }
        Err(e) => {
            errors.push(format!("restore_init_state: apply failed: {e}"));
            0
        }
    }
}

/// Per-Entity cursors folded out of a restore payload's change log.
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
/// the cursors a restored Entity resumes from.
fn restore_cursors(change_log_bytes: &[u8]) -> RestoreCursors {
    let mut cursors = RestoreCursors::default();
    for record in iter_change_log_records(change_log_bytes) {
        let Ok(record) = record else { break };
        if record.header.get(9) == Some(&crate::entity::header::FORMAT_ID_EFFECT_RECORD) {
            if let Ok(body) = postcard::from_bytes::<crate::entity::flusher::EffectRecordBody>(
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

/// Scan every live operative's entities for the slot whose wire identity equals
/// `entity_id`. The SDK encodes `entity_identity` with the same framing
/// the controller uses for `entity_id` (`[rc_len][rc][sh_len][sh][pk_len][pk]`),
/// so a byte-equal match is exact. Returns a cloned `Arc` so the operative-registry
/// locks are released before the (potentially slow) state rebuild.
pub(crate) fn find_entity_slot(runner: &RunnerNode, entity_id: &[u8]) -> Option<Arc<EntitySlot>> {
    for instance in runner.get_operatives().values() {
        for procedure_entities in instance.entities.iter() {
            for slot in procedure_entities.iter() {
                if slot.entity_identity.as_slice() == entity_id {
                    return Some(Arc::clone(slot));
                }
            }
        }
    }

    None
}
