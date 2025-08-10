use super::CommandAction;
use crate::operator::OperatorNode;
use crate::pb::MigrationSerialization;
use crate::processor::factory_registry::ProcessorHandle;

impl CommandAction for MigrationSerialization {
    fn handle(
        &self,
        operator: &mut OperatorNode,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let processor_handle: Option<ProcessorHandle> =
            operator.get_processor(self.processor_id.clone());

        let serialized_state = match processor_handle {
            Some(p) => {
                let processor = p.processor_mut();
                processor.json_serialize_state().unwrap()
            }
            None => Vec::new(),
        };

        // Remove the processor from the operator
        operator.stop_processor(self.processor_id.clone());

        ("migration_serialization".to_string(), serialized_state)
    }
}
