//! `MigrationSerialization` — produces the per-Shard / per-Partition
//! state blob per `relay-mvp-rust/design.md` Decision 10, then removes
//! the Relay instance from the local registry (the controller is migrating
//! it elsewhere).

use super::CommandAction;
use crate::command::partition_state::serialize_relay_state;
use crate::executor::ExecutorNode;
use crate::pb::MigrationSerialization;

impl CommandAction for MigrationSerialization {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let serialized_state = match executor.get_relay(&self.relay_id) {
            Some(instance) => serialize_relay_state(&instance),
            None => Vec::new(),
        };

        executor.stop_relay(&self.relay_id);

        ("migration_serialization".to_string(), serialized_state)
    }
}
