use super::CommandAction;
use crate::executor::ExecutorNode;
use crate::pb::RemOperative;

impl CommandAction for RemOperative {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        if executor.has_operative(&self.operative_id) {
            executor.stop_operative(&self.operative_id);
            ("stop_operative".to_string(), Vec::new())
        } else {
            errors.push("UnboundedOperative instance not found".to_string());
            ("stop_operative".to_string(), Vec::new())
        }
    }
}
