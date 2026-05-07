//! Inbound `RestoreInitState`: agent → SDK warm restart of a Partition.
//!
//! v1 v0 wiring: decodes the framed change-log to confirm the agent's
//! payload is well-formed, then replies with the highest seq encountered
//! as `terminal_seq`. The full integration with
//! `init_partition_with_payload` happens at `RelayInstance::new` time
//! (the dispatcher's restoration path) — this handler is the
//! control-channel surface that returns the SDK's terminal seq once the
//! restore has been applied to the live `PartitionSlot`.

use super::CommandAction;
use crate::compute_engine::ComputeEngineNode;
use crate::partition::restore::iter_change_log_records;
use crate::pb::{RestoreInitState, RestoreInitStateResponse};
use prost::Message;

impl CommandAction for RestoreInitState {
    fn handle(
        &self,
        _compute_engine: &mut ComputeEngineNode,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let mut terminal_seq: u64 = 0;

        for record in iter_change_log_records(&self.change_log_bytes) {
            match record {
                Ok(r) => terminal_seq = terminal_seq.max(r.seq),
                Err(e) => {
                    errors.push(format!("change_log decode error: {e}"));
                    return ("restore_init_state".to_string(), Vec::new());
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
