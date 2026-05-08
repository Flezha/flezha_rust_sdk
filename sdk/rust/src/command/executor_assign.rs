use super::CommandAction;
use crate::{
    executor::{ExecutorNode, ExecutorStatus},
    pb::AssignExecutorId,
};

impl CommandAction for AssignExecutorId {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        executor.assign_executor_id(self.executor_id.clone());
        executor.set_status(ExecutorStatus::Running);

        ("assign_executor_id".to_string(), Vec::new())
    }
}
