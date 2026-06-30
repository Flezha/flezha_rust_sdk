//! Inbound `RestoreInitState`: controller → SDK warm restart of a Segment.
//!
//! The controller ships the persisted snapshot + WAL change-log for a
//! Segment after an executor restart / rebind. This handler locates the
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
use crate::executor::ExecutorNode;
use crate::pb::{RestoreInitState, RestoreInitStateResponse};
use crate::segment::restore::iter_change_log_records;
use crate::segment::SegmentSlot;

impl CommandAction for RestoreInitState {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let slot = find_segment_slot(executor, &self.segment_id);

        let mut terminal_seq: u64 = 0;

        match slot {
            Some(slot) => {
                let mut guard = match slot.state.lock() {
                    Ok(g) => g,
                    Err(poisoned) => poisoned.into_inner(),
                };

                match slot.dispatcher.restore_state(
                    &mut guard,
                    &self.snapshot_bytes,
                    &self.change_log_bytes,
                    slot.operation_key.as_deref(),
                ) {
                    Ok(t) => {
                        terminal_seq = t;
                        drop(guard);
                        // Next produced frame must continue at terminal+1 so the
                        // controller sees a contiguous seq and skips epoch realign.
                        slot.change_seq.store(t + 1, Ordering::Release);
                    }
                    Err(e) => {
                        errors.push(format!("restore_init_state: apply failed: {e}"));
                    }
                }
            }

            None => {
                // No live slot — validate the change-log and ack the terminal
                // seq (the controller still advances its handshake).
                for record in iter_change_log_records(&self.change_log_bytes) {
                    match record {
                        Ok(r) => terminal_seq = terminal_seq.max(r.seq),
                        Err(e) => {
                            errors.push(format!("change_log decode error: {e}"));
                            return ("restore_init_state".to_string(), Vec::new());
                        }
                    }
                }
            }
        }

        let response = RestoreInitStateResponse {
            segment_id: self.segment_id.clone(),
            terminal_seq,
        };

        ("restore_init_state".to_string(), response.encode_to_vec())
    }
}

/// Scan every live operative's segments for the slot whose wire identity equals
/// `segment_id`. The SDK encodes `segment_identity` with the same framing
/// the controller uses for `segment_id` (`[rc_len][rc][sh_len][sh][pk_len][pk]`),
/// so a byte-equal match is exact. Returns a cloned `Arc` so the operative-registry
/// locks are released before the (potentially slow) state rebuild.
fn find_segment_slot(executor: &ExecutorNode, segment_id: &[u8]) -> Option<Arc<SegmentSlot>> {
    let view = executor.get_operatives();
    let guard = match view.read() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };

    for inst_mutex in guard.values() {
        let instance = match inst_mutex.lock() {
            Ok(g) => Arc::clone(&g),
            Err(poisoned) => Arc::clone(&poisoned.into_inner()),
        };

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
