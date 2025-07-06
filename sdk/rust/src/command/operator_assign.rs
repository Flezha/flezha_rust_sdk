use super::CommandAction;
use crate::node::{OperatorNode, OperatorStatus};
use crate::pb::AssignOperatorId;
use std::sync::{Arc, Mutex};

impl CommandAction for AssignOperatorId {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        // TODO: Implement error handling.

        let mut operator_guard = operator.lock().unwrap();
        operator_guard.assign_operator_id(self.operator_id.clone());
        operator_guard.set_status(OperatorStatus::Running);

        ("assign_operator_id".to_string(), Vec::new())
    }
}
