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
        _errors: &mut Vec<String>
    ) -> (String, String) {       
        // TODO: Implement error handling.         
        let operator_guard = operator.lock().unwrap();
        operator_guard.rem_pipeline(self.pipeline_id.clone());        
        
        ("rem_pipeline".to_string(), "Pipeline has been removed".to_string())
    }
}
