use super::CommandAction;
use crate::pb::OperatorDisconnect;
use std::sync::{Arc, Mutex};

use crate::node::{OperatorNode, OperatorStatus};

impl CommandAction for OperatorDisconnect {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        _errors: &mut Vec<String>
    ) -> (String, Vec<u8>) {   
        let mut operator_guard = operator.lock().unwrap();
        operator_guard.set_status(OperatorStatus::Stopped);

        ("operator_disconnect".to_string(), Vec::new())
    }
}