use super::CommandAction;
use crate::computation::factory_registry::ComputationHandle;
use crate::operator::OperatorNode;
use crate::pb::MigrationSerialization;

impl CommandAction for MigrationSerialization {
    fn handle(
        &self,
        operator: &mut OperatorNode,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let computation_handle: Option<ComputationHandle> =
            operator.get_computation(self.computation_id.clone());

        let serialized_state = match computation_handle {
            Some(p) => {
                let computation = p.computation_mut();
                computation.json_serialize_state().unwrap()
            }
            None => Vec::new(),
        };

        // Remove the computation from the operator
        operator.stop_computation(self.computation_id.clone());

        ("migration_serialization".to_string(), serialized_state)
    }
}
