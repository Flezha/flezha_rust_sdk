use super::CommandAction;
use crate::{operator::OperatorNode, pb::RemProcessor};

impl CommandAction for RemProcessor {
    fn handle(
        &self,
        operator: &mut OperatorNode,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        if operator.has_processor(self.processor_id.clone()) {
            operator.stop_processor(self.processor_id.clone());

            ("stop_processor".to_string(), Vec::new())
        } else {
            errors.push("Processor not found".to_string());
            ("stop_processor".to_string(), Vec::new())
        }
    }
}
