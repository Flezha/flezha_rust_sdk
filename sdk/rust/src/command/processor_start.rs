use super::CommandAction;

use serde_json::Value;

use crate::compute_engine::ComputeEngineNode;
use crate::pb::AddProcessor;
use crate::processor::factory_registry::get_factory;

#[cfg(test)]
mod tests;

impl CommandAction for AddProcessor {
    fn handle(
        &self,
        compute_engine: &mut ComputeEngineNode,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let processor_id = self.processor_id.clone();

        // Idempotent on reconnect: the agent re-issues AddProcessor for every
        // processor ProcessHub still has tagged to this operator's profile
        // after a restart. If the processor is already in the local registry,
        // skip without re-registering — keeps the existing processor's state
        // intact and prevents duplicate-init crashes.
        if compute_engine.has_processor(processor_id.clone()) {
            eprintln!(
                "[flezha] AddProcessor: processor {} already registered, skipping",
                processor_id
            );
            return ("start_processor".to_string(), Vec::new());
        }

        // Make sure factory exist.
        match get_factory(self.factory_id.clone()) {
            Some(_) => {}
            None => errors.push(format!(
                "Factory with id {} does not exist",
                self.factory_id.clone()
            )),
        };

        let processor_opt = compute_engine.start_processor(self.factory_id.clone(), processor_id);

        if errors.len() == 0 {
            match processor_opt {
                Some(processor_handle) => {
                    let mut processor = processor_handle.processor_mut();

                    if !self.init_values.is_empty() {
                        match serde_json::from_str::<Value>(&self.init_values) {
                            Ok(json_value) => {
                                if json_value.is_object()
                                    && !json_value.as_object().unwrap().is_empty()
                                {
                                    let default_values: Vec<u8> = serde_json::to_vec(&json_value)
                                        .expect("Failed to serialize init values");

                                    processor
                                        .json_deserialize_state(&default_values)
                                        .expect("Failed to deserialize state");
                                }
                            }
                            Err(e) => {
                                errors.push(format!("Failed to parse init_values as JSON: {}", e));
                            }
                        }
                    }

                    processor.post_alter();

                    return ("start_processor".to_string(), Vec::new());
                }
                None => errors.push(format!("Unable to create processor")),
            }
        }

        ("start_processor".to_string(), Vec::new())
    }
}
