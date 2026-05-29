use super::CommandAction;
use crate::executor::{persistent_id, ExecutorNode, ExecutorStatus};
use crate::pb::AssignExecutorId;

impl CommandAction for AssignExecutorId {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        executor.assign_executor_id(self.executor_id.clone());
        executor.set_status(ExecutorStatus::Running);
        // Persist node-locally so a fresh process can announce this id as
        // `previous_executor_id` and let the controller evict the stale
        // binding before re-binding.
        persistent_id::persist(&self.executor_id);

        ("assign_executor_id".to_string(), Vec::new())
    }
}
