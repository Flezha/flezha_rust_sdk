use super::CommandAction;

use std::sync::{Arc, Mutex};
use serde::Deserialize;

use crate::node::{OperatorNode, OperatorStatus};

#[derive(Deserialize, Debug)]
pub struct OperatorShutdownCommand {}

impl CommandAction for OperatorShutdownCommand {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        _errors: &mut Vec<String>
    ) -> (String, String) {   
        let mut operator_guard = operator.lock().unwrap();
        operator_guard.set_status(OperatorStatus::Stopped);

        ("operator_shutdown".to_string(), "Operator closing..".to_string())
    }
}