//! Inbound `RestoreInitState`: controller → SDK warm restart of a Partition.
//!
//! The controller ships the persisted snapshot + WAL change-log for a
//! Partition after an executor restart / rebind. This handler locates the
//! live `PartitionSlot` whose wire identity matches `partition_id` and
//! rebuilds its (type-erased) state in place via the dispatcher's
//! `restore_state` — so a relay that summed to N comes back at N instead of
//! starting empty. It then re-arms the partition's `change_seq` to
//! `terminal_seq + 1` so the next produced frame is contiguous with the
//! persisted history and the controller does not realign (truncate) the WAL.
//!
//! When no live slot matches yet (the relay has not been re-added), it falls
//! back to validating the change-log and acking the terminal seq.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use prost::Message;

use super::CommandAction;
use crate::executor::ExecutorNode;
use crate::partition::restore::iter_change_log_records;
use crate::partition::PartitionSlot;
use crate::pb::{RestoreInitState, RestoreInitStateResponse};

impl CommandAction for RestoreInitState {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let slot = find_partition_slot(executor, &self.partition_id);

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
                    slot.shard_key.as_deref(),
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
            partition_id: self.partition_id.clone(),
            terminal_seq,
        };

        ("restore_init_state".to_string(), response.encode_to_vec())
    }
}

/// Scan every live relay's partitions for the slot whose wire identity equals
/// `partition_id`. The SDK encodes `partition_identity` with the same framing
/// the controller uses for `partition_id` (`[rc_len][rc][sh_len][sh][pk_len][pk]`),
/// so a byte-equal match is exact. Returns a cloned `Arc` so the relay-registry
/// locks are released before the (potentially slow) state rebuild.
fn find_partition_slot(executor: &ExecutorNode, partition_id: &[u8]) -> Option<Arc<PartitionSlot>> {
    let view = executor.get_relays();
    let guard = match view.read() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };

    for inst_mutex in guard.values() {
        let instance = match inst_mutex.lock() {
            Ok(g) => Arc::clone(&g),
            Err(poisoned) => Arc::clone(&poisoned.into_inner()),
        };

        for shard_partitions in instance.partitions.iter() {
            for slot in shard_partitions.iter() {
                if slot.partition_identity.as_slice() == partition_id {
                    return Some(Arc::clone(slot));
                }
            }
        }
    }

    None
}
