use super::CommandAction;

use serde_json::Value;
use std::sync::{Arc, Mutex};

use crate::computation::factory_registry::get_factory;
use crate::node::OperatorNode;
use crate::pb::AddComputation;

impl CommandAction for AddComputation {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let operator_guard = operator.lock().unwrap();
        let computation_id = self.computation_id.clone();

        // Make sure factory exist.
        match get_factory(self.factory_id.clone()) {
            Some(_) => {}
            None => errors.push(format!(
                "Factory with id {} does not exist",
                self.factory_id.clone()
            )),
        };

        let computation_opt =
            operator_guard.start_computation(self.factory_id.clone(), computation_id);

        if errors.len() == 0 {
            match computation_opt {
                Some(computation_handle) => {
                    let mut computation = computation_handle.computation_mut();

                    if !self.init_values.is_empty() {
                        match serde_json::from_str::<Value>(&self.init_values) {
                            Ok(json_value) => {
                                if json_value.is_object()
                                    && !json_value.as_object().unwrap().is_empty()
                                {
                                    let default_values: Vec<u8> = serde_json::to_vec(&json_value)
                                        .expect("Failed to serialize init values");

                                    computation
                                        .json_deserialize_state(&default_values)
                                        .expect("Failed to deserialize state");
                                }
                            }
                            Err(e) => {
                                errors.push(format!("Failed to parse init_values as JSON: {}", e));
                            }
                        }
                    }

                    computation.post_alter();

                    return ("start_computation".to_string(), Vec::new());
                }
                None => errors.push(format!("Unable to create computation")),
            }
        }

        ("start_computation".to_string(), Vec::new())
    }
}
