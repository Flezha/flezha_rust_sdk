use super::CommandAction;
use crate::{executor::ExecutorNode, pb::RemRelay};

impl CommandAction for RemRelay {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        if executor.has_relay(&self.relay_id) {
            executor.stop_relay(&self.relay_id);
            ("stop_relay".to_string(), Vec::new())
        } else {
            errors.push("Relay instance not found".to_string());
            ("stop_relay".to_string(), Vec::new())
        }
    }
}
