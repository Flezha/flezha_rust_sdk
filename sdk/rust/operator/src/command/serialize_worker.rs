use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};

use super::CommandAction;

use serde::Deserialize;
use serde_json::Value;

use crate::node::OperatorNode;
use crate::service::payloader;

#[derive(Deserialize)]
pub struct SerializeWorkerCommand {
    worker_id: String
}

impl CommandAction for SerializeWorkerCommand {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        reference_id: String,
    ) {
        let operator_guard = operator.lock().unwrap();
        let control_stream = operator_guard.get_control_stream();
        let workers_lock = operator_guard.get_workers();
        let workers = workers_lock.read().unwrap();

        drop(operator_guard);

        let serialized_json = match workers.get(&self.worker_id) {
            Some(comp_arc) => {
                let worker = &*comp_arc.lock().unwrap();
                let serialized_data = worker.serialize_worker();

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
        let payload = payloader::build_from_str(&"serialize_worker".to_string(), &json_string);
        control_stream.try_clone().unwrap().write_all(&payload).unwrap();
    }
}