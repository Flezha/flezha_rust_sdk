use super::CommandAction;
use crate::node::OperatorNode;
use crate::pb::MigrationDeserialization;
use std::sync::{Arc, Mutex};

impl CommandAction for MigrationDeserialization {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let operator_guard = operator.lock().unwrap();

        // TODO: Implement error handling.

        let pipeline_handle = operator_guard
            .start_pipeline(self.factory_id.clone(), self.computation_id.clone())
            .unwrap();

        drop(operator_guard);

        let mut pipeline = pipeline_handle.pipeline_mut();

        let _ = pipeline.json_deserialize_state(&self.serialized_state.clone());

        ("migration_deserialization".to_string(), Vec::new())
    }
}
