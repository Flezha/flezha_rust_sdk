use super::CommandAction;
use crate::computation::factory_registry::ComputationHandle;
use crate::node::OperatorNode;
use crate::pb::MigrationSerialization;
use std::sync::{Arc, Mutex};

impl CommandAction for MigrationSerialization {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let operator_guard = operator.lock().unwrap();
        let computation_handle: Option<ComputationHandle> =
            operator_guard.get_computation(self.computation_id.clone());

        let serialized_state = match computation_handle {
            Some(p) => {
                let computation = p.computation_mut();
                computation.json_serialize_state().unwrap()
            }
            None => Vec::new(),
        };

        // Remove the computation from the operator
        operator_guard.stop_computation(self.computation_id.clone());

        drop(operator_guard);

        ("migration_serialization".to_string(), serialized_state)
    }
}
