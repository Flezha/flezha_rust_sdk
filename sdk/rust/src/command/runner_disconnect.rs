use super::CommandAction;
use crate::pb::RunnerDisconnect;
use crate::runner::{RunnerNode, RunnerStatus};

impl CommandAction for RunnerDisconnect {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        runner.set_status(RunnerStatus::Stopped);

        ("runner_disconnect".to_string(), Vec::new())
    }
}
