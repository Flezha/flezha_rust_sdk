use std::sync::{Arc, Mutex};
use crate::node::OperatorNode;
use crate::pipeline::factory_registry::PipelineHandle;
use crate::pb::PipelineState;
use super::CommandAction;

impl CommandAction for PipelineState {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        errors: &mut Vec<String>
    ) -> (String, Vec<u8>) {   
        let operator_guard = operator.lock().unwrap();
        let pipeline_handle: Option<PipelineHandle> = operator_guard.get_pipeline(self.pipeline_id.clone());
        
        match pipeline_handle {
            Some(pipeline_handle) => {
                let pipeline = pipeline_handle.pipeline_mut();
                let state_value = pipeline.get_state();

                (
                    "pipeline_state".to_string(),
                    serde_json::to_vec(&state_value).unwrap()
                )  
            },
            None => {
                let error_message = format!("Pipeline with id {} not found", self.pipeline_id);
                errors.push(error_message.clone());
                serde_json::to_string(&"Unable to get pipeline state.".to_string()).unwrap();

                (
                    "pipeline_state".to_string(),
                    Vec::new()
                )
            }
        }
    }
}
