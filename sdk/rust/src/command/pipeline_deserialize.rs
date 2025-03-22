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
        _errors: &mut Vec<String>
    ) -> (String, String) {      
        let operator_guard = operator.lock().unwrap();
        
        // TODO: Implement error handling.

        let pipeline_handle = operator_guard.add_pipeline(
            self.factory_id.clone(),
            self.pipeline_id.clone()
        ).unwrap();

        drop(operator_guard);

        let mut pipeline = pipeline_handle.pipeline_mut();

        let _ = pipeline.deserialize_state(&self.serialized_state.clone());

        (
            "deserialize_pipeline".to_string(),
            "Pipeline has been deserialized".to_string()
        )
    }
}