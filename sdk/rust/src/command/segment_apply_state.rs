//! Inverse of `segment_state`. Section 4 routes the state binary
//! through the dispatcher's per-Operation / per-Segment apply path.
//! Section 3 ships the wiring stub: any `bin_apply_state` request
//! returns the canonical `apply_state_not_supported` token until the
//! dispatcher exposes its apply hook.

use super::CommandAction;
use crate::pb::SegmentApplyState;
use crate::runner::RunnerNode;

impl CommandAction for SegmentApplyState {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        if !runner.has_operative(&self.operative_id) {
            errors.push(format!(
                "UnboundedOperative instance with id {} not found",
                self.operative_id
            ));
            return ("segment_apply_state".to_string(), Vec::new());
        }

        // Section 4: invoke the dispatcher's apply entry point.
        errors.push("apply_state_not_supported".to_string());
        ("segment_apply_state".to_string(), Vec::new())
    }
}
