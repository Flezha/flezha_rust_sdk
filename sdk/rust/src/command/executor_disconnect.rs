use super::CommandAction;
use crate::{
    executor::{ExecutorNode, ExecutorStatus},
    pb::ExecutorDisconnect,
};

impl CommandAction for ExecutorDisconnect {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        executor.set_status(ExecutorStatus::Stopped);

        ("executor_disconnect".to_string(), Vec::new())
    }
}
