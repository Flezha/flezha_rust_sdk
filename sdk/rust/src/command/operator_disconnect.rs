use super::CommandAction;
use crate::{
    operator::{OperatorNode, OperatorStatus},
    pb::OperatorDisconnect,
};

impl CommandAction for OperatorDisconnect {
    fn handle(
        &self,
        operator: &mut OperatorNode,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        operator.set_status(OperatorStatus::Stopped);

        ("operator_disconnect".to_string(), Vec::new())
    }
}
