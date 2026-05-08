//! Inverse of `partition_state`. Section 4 routes the state binary
//! through the dispatcher's per-Shard / per-Partition apply path.
//! Section 3 ships the wiring stub: any `bin_apply_state` request
//! returns the canonical `apply_state_not_supported` token until the
//! dispatcher exposes its apply hook.

use super::CommandAction;
use crate::executor::ExecutorNode;
use crate::pb::PartitionApplyState;

impl CommandAction for PartitionApplyState {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        if !executor.has_relay(&self.relay_id) {
            errors.push(format!(
                "Relay instance with id {} not found",
                self.relay_id
            ));
            return ("partition_apply_state".to_string(), Vec::new());
        }

        // Section 4: invoke the dispatcher's apply entry point.
        errors.push("apply_state_not_supported".to_string());
        ("partition_apply_state".to_string(), Vec::new())
    }
}
