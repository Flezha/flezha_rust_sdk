use super::CommandAction;

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use serde::Deserialize;

use crate::node::OperatorNode;
use crate::service::payloader;

#[derive(Deserialize, Debug)]
pub struct ComputationStateCommand {
    computation_id: String
}

impl CommandAction for ComputationStateCommand {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        reference_id: String,
    ) {
        let operator_guard = operator.lock().unwrap();
        let computation = operator_guard.get_computation(self.computation_id.clone()).unwrap();
        let state_json = computation.lock().unwrap().to_json_string();

        // TODO: the same code is duplicated in other commands also.
        let mut hashmap: HashMap<String, String> = HashMap::new();
        hashmap.insert("reference_id".to_string(), reference_id);
        hashmap.insert("data".to_string(), state_json);

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_from_str(&"computation_state".to_string(), &json_string);
        operator_guard.get_control_stream().try_clone().unwrap().write_all(&payload).unwrap();
    }
}
