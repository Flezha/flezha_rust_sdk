use super::CommandAction;

use crate::compute_engine::ComputeEngineNode;
use crate::pb::AddProcessor;

impl CommandAction for AddProcessor {
    fn handle(
        &self,
        compute_engine: &mut ComputeEngineNode,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let instance_id = self.processor_id.clone();
        let factory_id = self.factory_id.clone();

        // Idempotent on reconnect: the agent re-issues AddProcessor for
        // every Relay instance ProcessHub still has tagged to this
        // operator's profile after a restart. If the instance is already
        // in the local registry, skip without re-registering — keeps
        // the existing per-Partition state intact and prevents
        // duplicate-init crashes.
        if compute_engine.has_relay(&instance_id) {
            eprintln!(
                "[flezha] AddProcessor: relay instance {} already registered, skipping",
                instance_id
            );
            return ("start_processor".to_string(), Vec::new());
        }

        match compute_engine.start_relay(factory_id.clone(), instance_id.clone()) {
            Some(instance) => {
                if !self.init_values.is_empty() {
                    if let Err(e) = crate::command::processor_state::apply_init_values(
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

        ("start_processor".to_string(), Vec::new())
    }
}
