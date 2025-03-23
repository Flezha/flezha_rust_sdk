use super::CommandAction;

use std::sync::{Arc, Mutex};
use serde::Deserialize;

use crate::node::OperatorNode;

#[derive(Deserialize, Debug)]
pub struct RemPipelineCommand {
    pipeline_id: String
}

impl CommandAction for RemPipelineCommand {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        errors: &mut Vec<String>
    ) -> (String, String) {       
        let operator_guard = operator.lock().unwrap();

        if operator_guard.has_pipeline(self.pipeline_id.clone()) {
            operator_guard.rem_pipeline(self.pipeline_id.clone());

            ("rem_pipeline".to_string(), "Pipeline has been removed".to_string())
        } else {
            errors.push("Pipeline not found".to_string());
            ("rem_pipeline".to_string(), "Pipeline not found".to_string())
        }        
    }
}
