use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};

use super::CommandAction;

use serde::Deserialize;
use serde_json::Value;

use crate::node::OperatorNode;
use crate::service::payloader;

#[derive(Deserialize)]
pub struct DeSerializePipelineCommand {
    pipeline_id: String,
    factory_id: String,
    serialized_state: Vec<u8>
}

impl CommandAction for DeSerializePipelineCommand {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        reference_id: String,
    ) {
        let operator_guard = operator.lock().unwrap();
        let control_stream = operator_guard.get_control_stream();
        
        let pipeline_handle = operator_guard.add_pipeline(
            self.factory_id.clone(),
            self.pipeline_id.clone()
        ).unwrap();

        drop(operator_guard);

        let mut pipeline = pipeline_handle.pipeline_mut();

        let _ = pipeline.deserialize_state(&self.serialized_state.clone());

        // TODO: the same code is all over the place, refactor it.
        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_from_str(&"deserialize_pipeline".to_string(), &json_string);
        control_stream.try_clone().unwrap().write_all(&payload).unwrap();
    }
}