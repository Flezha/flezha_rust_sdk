use super::CommandAction;
use crate::pb::RemOperative;
use crate::runner::RunnerNode;

impl CommandAction for RemOperative {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        if runner.has_operative(&self.operative_id) {
            runner.stop_operative(&self.operative_id);
            ("stop_operative".to_string(), Vec::new())
        } else {
            errors.push("UnboundedOperative instance not found".to_string());
            ("stop_operative".to_string(), Vec::new())
        }
    }
}
