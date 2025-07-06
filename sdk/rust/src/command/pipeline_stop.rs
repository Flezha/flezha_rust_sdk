use super::CommandAction;

use std::sync::{Arc, Mutex};

use crate::node::OperatorNode;
use crate::pb::RemPipeline;

impl CommandAction for RemPipeline {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let operator_guard = operator.lock().unwrap();

        if operator_guard.has_pipeline(self.pipeline_id.clone()) {
            operator_guard.stop_pipeline(self.pipeline_id.clone());

            ("stop_pipeline".to_string(), Vec::new())
        } else {
            errors.push("Pipeline not found".to_string());
            ("stop_pipeline".to_string(), Vec::new())
        }
    }
}
