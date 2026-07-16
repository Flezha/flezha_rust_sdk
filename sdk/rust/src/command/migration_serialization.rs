//! `MigrationSerialization` — produces the per-Operation / per-Segment
//! state blob per `operative-mvp-rust/design.md` Decision 10, then removes
//! the UnboundedOperative instance from the local registry (the controller is migrating
//! it elsewhere).

use super::CommandAction;
use crate::command::segment_state::serialize_operative_state;
use crate::pb::MigrationSerialization;
use crate::runner::RunnerNode;

impl CommandAction for MigrationSerialization {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let serialized_state = match runner.get_unbounded(&self.operative_id) {
            Some(instance) => serialize_operative_state(&instance),
            None => Vec::new(),
        };

        runner.stop_operative(&self.operative_id);

        ("migration_serialization".to_string(), serialized_state)
    }
}
