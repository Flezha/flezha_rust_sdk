//! `MigrationDeserialization` — restore a Relay instance's per-Shard /
//! per-Partition state from a snapshot blob and register it locally.
//! Section 4 wires the actual state-restoration path through the
//! dispatcher's `apply_state` entry point; section 3 ships the
//! wiring so the wire command continues to round-trip.

use super::CommandAction;
use crate::executor::ExecutorNode;
use crate::pb::MigrationDeserialization;

impl CommandAction for MigrationDeserialization {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        match executor.start_relay(self.factory_id.clone(), self.relay_id.clone()) {
            Some(_instance) => {
                // Section 4 consumes self.serialized_state and applies
                // per-Shard / per-Partition state via the dispatcher's
                // State-erasure layer. Until then we accept the bytes
                // and discard them so the controller's migration handshake
                // completes; the new instance starts with init_state.
                ("migration_deserialization".to_string(), Vec::new())
            }
            None => {
                errors.push(format!(
                    "Relay class with factory_id {} is not registered",
                    self.factory_id
                ));
                ("migration_deserialization".to_string(), Vec::new())
            }
        }
    }
}
