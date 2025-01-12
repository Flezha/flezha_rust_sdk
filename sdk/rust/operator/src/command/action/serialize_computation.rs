use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};

use super::CommandAction;

use serde::Deserialize;
use serde_json::Value;

use crate::node::WorkerNode;
use crate::service::payloader;

#[derive(Deserialize)]
pub struct SerializeComputationCommand {
    computation_id: String
}

impl CommandAction for SerializeComputationCommand {
    fn handle(
        &self,
        operator: &Arc<Mutex<WorkerNode>>,
        _message: &String,
        reference_id: String,
    ) {
        let operator_guard = operator.lock().unwrap();
        let control_stream = operator_guard.get_control_stream();
        let computations_lock = operator_guard.get_computations();
        let computations = computations_lock.read().unwrap();

        drop(operator_guard);

        let serialized_json = match computations.get(&self.computation_id) {
            Some(comp_arc) => {
                let computation = &*comp_arc.lock().unwrap();
                let serialized_data = computation.serialize_computation();

                let data_as_values = serialized_data.iter()
                    .map(|&byte| Value::Number(serde_json::Number::from(byte)))
                    .collect();
    
                Value::Array(data_as_values)
            }
            None => Value::Null,
        };

        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));
        hashmap.insert("data".to_string(), serialized_json);

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_from_str(&"serialize_computation".to_string(), &json_string);
        control_stream.try_clone().unwrap().write_all(&payload).unwrap();
    }
}