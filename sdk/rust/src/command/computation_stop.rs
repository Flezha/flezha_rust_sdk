use super::CommandAction;
use crate::{operator::OperatorNode, pb::RemComputation};

impl CommandAction for RemComputation {
    fn handle(
        &self,
        operator: &mut OperatorNode,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        if operator.has_computation(self.computation_id.clone()) {
            operator.stop_computation(self.computation_id.clone());

            ("stop_computation".to_string(), Vec::new())
        } else {
            errors.push("Computation not found".to_string());
            ("stop_computation".to_string(), Vec::new())
        }
    }
}
