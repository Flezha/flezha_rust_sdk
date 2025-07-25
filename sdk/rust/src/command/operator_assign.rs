use super::CommandAction;
use crate::{
    operator::{OperatorNode, OperatorStatus},
    pb::AssignOperatorId,
};

impl CommandAction for AssignOperatorId {
    fn handle(
        &self,
        operator: &mut OperatorNode,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        operator.assign_operator_id(self.operator_id.clone());
        operator.set_status(OperatorStatus::Running);

        ("assign_operator_id".to_string(), Vec::new())
    }
}
