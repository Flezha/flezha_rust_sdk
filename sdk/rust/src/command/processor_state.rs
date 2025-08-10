use super::CommandAction;
use crate::pb::ProcessorState;
use crate::{compute_engine::ComputeEngineNode, processor::factory_registry::ProcessorHandle};

impl CommandAction for ProcessorState {
    fn handle(
        &self,
        compute_engine: &mut ComputeEngineNode,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let processor_handle: Option<ProcessorHandle> =
            compute_engine.get_processor(self.processor_id.clone());

        match processor_handle {
            Some(processor_handle) => {
                let processor = processor_handle.processor_mut();
                let state_value = processor.bin_serialize_state();

                (
                    "processor_state".to_string(),
                    serde_json::to_vec(&state_value).unwrap(),
                )
            }
            None => {
                let error_message = format!("Processor with id {} not found", self.processor_id);
                errors.push(error_message.clone());
                serde_json::to_string(&"Unable to get processor state.".to_string()).unwrap();

                ("processor_state".to_string(), Vec::new())
            }
        }
    }
}
