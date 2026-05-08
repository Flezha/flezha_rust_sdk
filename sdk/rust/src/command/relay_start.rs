use super::CommandAction;

use crate::executor::ExecutorNode;
use crate::pb::AddRelay;

impl CommandAction for AddRelay {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let instance_id = self.relay_id.clone();
        let factory_id = self.factory_id.clone();

        // Idempotent on reconnect: the controller re-issues AddRelay for
        // every Relay instance ProcessHub still has tagged to this
        // executor's profile after a restart. If the instance is already
        // in the local registry, skip without re-registering — keeps
        // the existing per-Partition state intact and prevents
        // duplicate-init crashes.
        if executor.has_relay(&instance_id) {
            eprintln!(
                "[flezha] AddRelay: relay instance {} already registered, skipping",
                instance_id
            );
            return ("start_relay".to_string(), Vec::new());
        }

        match executor.start_relay(factory_id.clone(), instance_id.clone()) {
            Some(instance) => {
                if !self.init_values.is_empty() {
                    if let Err(e) = crate::command::partition_state::apply_init_values(
                        &instance,
                        &self.init_values,
                    ) {
                        errors.push(e);
                    }
                }
            }
            None => {
                errors.push(format!(
                    "Relay class with factory_id {} is not registered",
                    factory_id
                ));
            }
        }

        ("start_relay".to_string(), Vec::new())
    }
}
