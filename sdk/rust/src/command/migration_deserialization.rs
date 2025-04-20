use std::sync::{Arc, Mutex};
use serde::Deserialize;

use super::CommandAction;
use crate::node::OperatorNode;

#[derive(Deserialize)]
pub struct MigrationDeserializationCommand {
    pipeline_id: String,
    factory_id: String,
    serialized_state: Vec<u8>
}

impl CommandAction for MigrationDeserializationCommand {
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