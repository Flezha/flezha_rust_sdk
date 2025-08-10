use super::CommandAction;
use crate::compute_engine::ComputeEngineNode;
use crate::pb::MigrationSerialization;
use crate::processor::factory_registry::ProcessorHandle;

impl CommandAction for MigrationSerialization {
    fn handle(
        &self,
        compute_engine: &mut ComputeEngineNode,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let processor_handle: Option<ProcessorHandle> =
            compute_engine.get_processor(self.processor_id.clone());

        let serialized_state = match processor_handle {
            Some(p) => {
                let processor = p.processor_mut();
                processor.json_serialize_state().unwrap()
            }
            None => Vec::new(),
        };

        // Remove the processor from the compute_engine
        compute_engine.stop_processor(self.processor_id.clone());

        ("migration_serialization".to_string(), serialized_state)
    }
}
