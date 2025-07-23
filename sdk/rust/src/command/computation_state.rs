use super::CommandAction;
use crate::node::OperatorNode;
use crate::pb::ComputationState;
use crate::computation::factory_registry::ComputationHandle;
use std::sync::{Arc, Mutex};

impl CommandAction for ComputationState {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let operator_guard = operator.lock().unwrap();
        let computation_handle: Option<ComputationHandle> =
            operator_guard.get_computation(self.computation_id.clone());

        match computation_handle {
            Some(computation_handle) => {
                let computation = computation_handle.computation_mut();
                let state_value = computation.bin_serialize_state();

                (
                    "computation_state".to_string(),
                    serde_json::to_vec(&state_value).unwrap(),
                )
            }
            None => {
                let error_message = format!("Computation with id {} not found", self.computation_id);
                errors.push(error_message.clone());
                serde_json::to_string(&"Unable to get computation state.".to_string()).unwrap();

                ("computation_state".to_string(), Vec::new())
            }
        }
    }
}
