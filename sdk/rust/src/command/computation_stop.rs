use super::CommandAction;

use std::sync::{Arc, Mutex};

use crate::node::OperatorNode;
use crate::pb::RemComputation;

impl CommandAction for RemComputation {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let operator_guard = operator.lock().unwrap();

        if operator_guard.has_computation(self.computation_id.clone()) {
            operator_guard.stop_computation(self.computation_id.clone());

            ("stop_computation".to_string(), Vec::new())
        } else {
            errors.push("Computation not found".to_string());
            ("stop_computation".to_string(), Vec::new())
        }
    }
}
