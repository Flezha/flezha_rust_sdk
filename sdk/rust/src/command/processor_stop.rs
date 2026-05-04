use super::CommandAction;
use crate::{compute_engine::ComputeEngineNode, pb::RemProcessor};

impl CommandAction for RemProcessor {
    fn handle(
        &self,
        compute_engine: &mut ComputeEngineNode,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        if compute_engine.has_relay(&self.processor_id) {
            compute_engine.stop_relay(&self.processor_id);
            ("stop_processor".to_string(), Vec::new())
        } else {
            errors.push("Relay instance not found".to_string());
            ("stop_processor".to_string(), Vec::new())
        }
    }
}
