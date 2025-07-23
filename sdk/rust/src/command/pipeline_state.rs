use super::CommandAction;
use crate::node::OperatorNode;
use crate::pb::ComputationState;
use crate::pipeline::factory_registry::ComputationHandle;
use std::sync::{Arc, Mutex};

impl CommandAction for ComputationState {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let operator_guard = operator.lock().unwrap();
        let pipeline_handle: Option<ComputationHandle> =
            operator_guard.get_pipeline(self.pipeline_id.clone());

        match pipeline_handle {
            Some(pipeline_handle) => {
                let pipeline = pipeline_handle.pipeline_mut();
                let state_value = pipeline.bin_serialize_state();

                (
                    "pipeline_state".to_string(),
                    serde_json::to_vec(&state_value).unwrap(),
                )
            }
            None => {
                let error_message = format!("Computation with id {} not found", self.pipeline_id);
                errors.push(error_message.clone());
                serde_json::to_string(&"Unable to get pipeline state.".to_string()).unwrap();

                ("pipeline_state".to_string(), Vec::new())
            }
        }
    }
}
