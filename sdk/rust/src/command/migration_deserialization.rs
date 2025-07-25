use super::CommandAction;
use crate::{operator::OperatorNode, pb::MigrationDeserialization};

impl CommandAction for MigrationDeserialization {
    fn handle(
        &self,
        operator: &mut OperatorNode,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let computation_handle = operator
            .start_computation(self.factory_id.clone(), self.computation_id.clone())
            .unwrap();

        let mut computation = computation_handle.computation_mut();
        let _ = computation.json_deserialize_state(&self.serialized_state.clone());

        ("migration_deserialization".to_string(), Vec::new())
    }
}
