use super::CommandAction;
use crate::executor::{ExecutorNode, ExecutorStatus};
use crate::pb::AssignExecutorId;

impl CommandAction for AssignExecutorId {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        // The controller-minted value is this connection's **session token**: it
        // keys the per-connection data-stream sockets and the controller-side
        // Registry/StreamChannel names. It is NOT persisted — the durable
        // identity is the self-owned stable id (see `persistent_id`), which the
        // SDK already presented in the connect handshake.
        executor.assign_executor_id(self.executor_id.clone());
        executor.set_status(ExecutorStatus::Running);

        ("assign_executor_id".to_string(), Vec::new())
    }
}
