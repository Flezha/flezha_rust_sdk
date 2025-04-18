use super::CommandAction;

use std::sync::{Arc, Mutex};
use serde::Deserialize;

use crate::node::OperatorNode;
use crate::pipeline::factory_registry::PipelineHandle;

#[derive(Deserialize, Debug)]
pub struct PipelineStateCommand {
    pipeline_id: String
}

impl CommandAction for PipelineStateCommand {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        errors: &mut Vec<String>
    ) -> (String, String) {   
        let operator_guard = operator.lock().unwrap();
        let pipeline_handle: Option<PipelineHandle> = operator_guard.get_pipeline(self.pipeline_id.clone());
        let json_string = match pipeline_handle {
            Some(pipeline_handle) => {
                let pipeline = pipeline_handle.pipeline_mut();
                let state_value = pipeline.get_state();
                serde_json::to_string(&state_value).unwrap()
            },
            None => {
                let error_message = format!("Pipeline with id {} not found", self.pipeline_id);
                errors.push(error_message.clone());
                serde_json::to_string(&"Unable to get pipeline state.".to_string()).unwrap()
            }
        };

        (
            "pipeline_state".to_string(),
            json_string
        )
    }
}
