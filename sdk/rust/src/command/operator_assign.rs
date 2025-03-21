use super::CommandAction;

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use serde_json::Value;
use serde::Deserialize;

use crate::node::OperatorNode;
use crate::service::payloader;

#[derive(Deserialize, Debug)]
pub struct AssignOperatorIdCommand {
    operator_id: String,
}

impl CommandAction for AssignOperatorIdCommand {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        reference_id: String,
    ) {
        let mut operator_guard = operator.lock().unwrap(); 

        operator_guard.assign_operator_id(self.operator_id.clone());       
                    
        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_from_str(&"add_pipeline".to_string(), &json_string);

        operator_guard.get_control_stream().try_clone().unwrap().write_all(&payload).unwrap();
    }
}