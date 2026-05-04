//! `MigrationSerialization` — produces the per-Shard / per-Partition
//! state blob per `relay-mvp-rust/design.md` Decision 10, then removes
//! the Relay instance from the local registry (the agent is migrating
//! it elsewhere).

use super::CommandAction;
use crate::command::processor_state::serialize_relay_state;
use crate::compute_engine::ComputeEngineNode;
use crate::pb::MigrationSerialization;

impl CommandAction for MigrationSerialization {
    fn handle(
        &self,
        compute_engine: &mut ComputeEngineNode,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let serialized_state = match compute_engine.get_relay(&self.processor_id) {
            Some(instance) => serialize_relay_state(&instance),
            None => Vec::new(),
        };

        compute_engine.stop_relay(&self.processor_id);

        ("migration_serialization".to_string(), serialized_state)
    }
}
