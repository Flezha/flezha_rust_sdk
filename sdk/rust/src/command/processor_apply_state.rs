//! Inverse of `processor_state`. Section 4 routes the state binary
//! through the dispatcher's per-Shard / per-Partition apply path.
//! Section 3 ships the wiring stub: any `bin_apply_state` request
//! returns the canonical `apply_state_not_supported` token until the
//! dispatcher exposes its apply hook.

use super::CommandAction;
use crate::compute_engine::ComputeEngineNode;
use crate::pb::ProcessorApplyState;

impl CommandAction for ProcessorApplyState {
    fn handle(
        &self,
        compute_engine: &mut ComputeEngineNode,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        if !compute_engine.has_relay(&self.processor_id) {
            errors.push(format!(
                "Relay instance with id {} not found",
                self.processor_id
            ));
            return ("processor_apply_state".to_string(), Vec::new());
        }

        // Section 4: invoke the dispatcher's apply entry point.
        errors.push("apply_state_not_supported".to_string());
        ("processor_apply_state".to_string(), Vec::new())
    }
}
