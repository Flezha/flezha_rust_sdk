use super::CommandAction;

use std::sync::{Arc, Mutex};
use serde::Deserialize;

use crate::node::{OperatorNode, OperatorStatus};

#[derive(Deserialize, Debug)]
pub struct AssignOperatorIdCommand {
    operator_id: String,
}

impl CommandAction for AssignOperatorIdCommand {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        _errors: &mut Vec<String>
    ) -> (String, String) {   
        // TODO: Implement error handling.
        
        let mut operator_guard = operator.lock().unwrap(); 
        operator_guard.assign_operator_id(self.operator_id.clone());    
        operator_guard.set_status(OperatorStatus::Running);
                    
        ("assign_operator_id".to_string(), "Operator assigned".to_string())
    }
}