//! `MigrationDeserialization` — restore a Relay instance's per-Shard /
//! per-Partition state from a snapshot blob and register it locally.
//! Reuses `apply_init_values` for the state-application path so a
//! migrated instance ends up with byte-identical Partition state to the
//! source instance produced by `serialize_relay_state`.

use super::CommandAction;
use crate::command::partition_state::apply_init_values;
use crate::executor::ExecutorNode;
use crate::pb::MigrationDeserialization;

impl CommandAction for MigrationDeserialization {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        match executor.start_relay(self.factory_id.clone(), self.relay_id.clone()) {
            Some(instance) => {
                if !self.serialized_state.is_empty() {
                    let json_str = match std::str::from_utf8(&self.serialized_state) {
                        Ok(s) => s,
                        Err(e) => {
                            errors.push(format!(
                                "MigrationDeserialization: serialized_state is not UTF-8: {}",
                                e
                            ));
                            return ("migration_deserialization".to_string(), Vec::new());
                        }
                    };
                    if let Err(e) = apply_init_values(&instance, json_str) {
                        errors.push(e);
                    }
                }
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
