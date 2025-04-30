use super::CommandAction;

use std::sync::{Arc, Mutex};
use serde::Deserialize;

use crate::node::OperatorNode;

#[derive(Deserialize, Debug)]
pub struct OperatorShutdownCommand {}

impl CommandAction for OperatorShutdownCommand {
    fn handle(
        &self,
        _operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        _errors: &mut Vec<String>
    ) -> (String, String) {   
                
        ("operator_shutdown".to_string(), "Operator closing..".to_string())
    }
}