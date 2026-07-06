//! `MigrationSerialization` — produces the per-Operation / per-Segment
//! state blob per `operative-mvp-rust/design.md` Decision 10, then removes
//! the UnboundedOperative instance from the local registry (the controller is migrating
//! it elsewhere).

use super::CommandAction;
use crate::command::segment_state::serialize_operative_state;
use crate::executor::ExecutorNode;
use crate::pb::MigrationSerialization;

impl CommandAction for MigrationSerialization {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let serialized_state = match executor.get_unbounded(&self.operative_id) {
            Some(instance) => serialize_operative_state(&instance),
            None => Vec::new(),
        };

        executor.stop_operative(&self.operative_id);

        ("migration_serialization".to_string(), serialized_state)
    }
}
