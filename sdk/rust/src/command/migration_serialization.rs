use super::CommandAction;
use crate::node::OperatorNode;
use crate::pb::MigrationSerialization;
use crate::pipeline::factory_registry::PipelineHandle;
use std::sync::{Arc, Mutex};

impl CommandAction for MigrationSerialization {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let operator_guard = operator.lock().unwrap();
        let pipeline_handle: Option<PipelineHandle> =
            operator_guard.get_pipeline(self.pipeline_id.clone());

        // TODO: Implement error handling.

        let serialized_state = match pipeline_handle {
            Some(p) => {
                let pipeline = p.pipeline_mut();
                pipeline.json_serialize_state().unwrap()
            }
            None => Vec::new(),
        };

        // Remove the pipeline from the operator
        operator_guard.stop_pipeline(self.pipeline_id.clone());

        drop(operator_guard);

        ("migration_serialization".to_string(), serialized_state)
    }
}
