use std::sync::{Arc, Mutex};
use super::CommandAction;
use crate::pb::MigrationDeserialization;
use crate::node::OperatorNode;

impl CommandAction for MigrationDeserialization {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        _errors: &mut Vec<String>
    ) -> (String, String) {      
        let operator_guard = operator.lock().unwrap();
        
        // TODO: Implement error handling.

        let pipeline_handle = operator_guard.start_pipeline(
            self.factory_id.clone(),
            self.pipeline_id.clone()
        ).unwrap();

        drop(operator_guard);

        let mut pipeline = pipeline_handle.pipeline_mut();

        let _ = pipeline.deserialize_state(&self.serialized_state.clone());

        (
            "migration_deserialization".to_string(),
            "Pipeline has been deserialized".to_string()
        )
    }
}