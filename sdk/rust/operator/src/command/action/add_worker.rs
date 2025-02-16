use super::CommandAction;

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use serde_json::Value;
use serde::Deserialize;

use crate::node::OperatorNode;
use crate::service::payloader;

#[derive(Deserialize, Debug)]
pub struct AddWorkerCommand {
    worker_id: String,
    factory_id: String,
    variables: Value,
}

impl CommandAction for AddWorkerCommand {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        reference_id: String,
    ) {
        let operator_guard = operator.lock().unwrap();
        let factory = operator_guard.get_factory(self.factory_id.clone()).unwrap();
        let variables = value_to_hashmap(&self.variables).unwrap();
        let mut worker = factory();
        
        worker.update_worker(&variables);
                
        operator_guard.add_worker(self.worker_id.clone(), worker);        
        
        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_from_str(&"add_worker".to_string(), &json_string);

        operator_guard.get_control_stream().try_clone().unwrap().write_all(&payload).unwrap();
    }
}

fn value_to_hashmap(value: &Value) -> Option<HashMap<String, Value>> {
    value.as_object().map(|map| {
        map.iter()
           .map(|(k, v)| (k.clone(), v.clone()))
           .collect()
    })
}