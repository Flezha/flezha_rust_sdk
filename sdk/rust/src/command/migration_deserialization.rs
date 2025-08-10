use super::CommandAction;
use crate::{operator::OperatorNode, pb::MigrationDeserialization};

impl CommandAction for MigrationDeserialization {
    fn handle(
        &self,
        operator: &mut OperatorNode,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let processor_handle = operator
            .start_processor(self.factory_id.clone(), self.processor_id.clone())
            .unwrap();

        let mut processor = processor_handle.processor_mut();
        let _ = processor.json_deserialize_state(&self.serialized_state.clone());

        ("migration_deserialization".to_string(), Vec::new())
    }
}
