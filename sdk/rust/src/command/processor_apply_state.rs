use super::CommandAction;
use crate::compute_engine::ComputeEngineNode;
use crate::pb::ProcessorApplyState;
use crate::processor::factory_registry::ProcessorHandle;

/// Inverse of `processor_state`. Receives an opaque SDK-serialised state
/// binary captured by a previous snapshot and feeds it to the processor's
/// `bin_apply_state` (or equivalent) before any input is delivered.
///
/// The default `Processor` impl returns `Err(ApplyNotSupported)`; concrete
/// processors override `bin_apply_state` to thread the binary through their
/// state-init pathway. When the SDK build does not implement Apply for a
/// given processor, the agent observes a non-empty `errors` list containing
/// the canonical token `apply_state_not_supported` and falls back to empty
/// default state.
impl CommandAction for ProcessorApplyState {
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
                let mut processor = processor_handle.processor_mut();

                match processor.bin_apply_state(&self.state_binary) {
                    Ok(()) => ("processor_apply_state".to_string(), Vec::new()),
                    Err(reason) => {
                        errors.push(reason);
                        ("processor_apply_state".to_string(), Vec::new())
                    }
                }
            }
            None => {
                let error_message = format!("Processor with id {} not found", self.processor_id);
                errors.push(error_message);
                ("processor_apply_state".to_string(), Vec::new())
            }
        }
    }
}
