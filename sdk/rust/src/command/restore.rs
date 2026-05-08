//! Inbound `RestoreInitState`: controller → SDK warm restart of a Partition.
//!
//! v1 v0 wiring: decodes the framed change-log to confirm the controller's
//! payload is well-formed, then replies with the highest seq encountered
//! as `terminal_seq`. The full integration with
//! `init_partition_with_payload` happens at `RelayInstance::new` time
//! (the dispatcher's restoration path) — this handler is the
//! control-channel surface that returns the SDK's terminal seq once the
//! restore has been applied to the live `PartitionSlot`.

use prost::Message;

use super::CommandAction;
use crate::executor::ExecutorNode;
use crate::partition::restore::iter_change_log_records;
use crate::pb::{RestoreInitState, RestoreInitStateResponse};

impl CommandAction for RestoreInitState {
    fn handle(
        &self,
        _executor: &mut ExecutorNode,
        _message: &str,
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
