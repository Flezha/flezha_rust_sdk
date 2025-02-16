use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};

use super::CommandAction;

use serde::Deserialize;
use serde_json::Value;

use crate::node::OperatorNode;
use crate::service::payloader;

#[derive(Deserialize)]
pub struct DeSerializeWorkerCommand {
    worker_id: String,
    serialized_data: Vec<u8>
}

impl CommandAction for DeSerializeWorkerCommand {
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

        match workers.get(&self.worker_id) {
            Some(comp_arc) => {
                let worker = comp_arc.lock().unwrap();

                worker.deserialize_worker(&self.serialized_data);
            }
            None => (),
        };

        // TODO: the same code is all over the place, refactor it.
        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_from_str(&"deserialize_worker".to_string(), &json_string);
        control_stream.try_clone().unwrap().write_all(&payload).unwrap();
    }
}