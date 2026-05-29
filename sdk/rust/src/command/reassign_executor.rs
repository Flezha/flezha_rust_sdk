use super::CommandAction;
use crate::executor::{persistent_id, ExecutorNode};
use crate::pb::ReassignExecutorId;

/// Controller -> SDK. Issued when the controller has reassigned this
/// SDK's bindings to a different executor session during a disconnect
/// window. The SDK updates `executor.executor_id` in place; no
/// registry mutation, no status change. Subsequent
/// `ExecutorReconnected` emissions carry the new id.
impl CommandAction for ReassignExecutorId {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        executor.assign_executor_id(self.new_id.clone());
        persistent_id::persist(&self.new_id);
        ("reassign_executor_id".to_string(), Vec::new())
    }
}
