use super::CommandAction;
use crate::pb::{ProcessorMessage, ProcessorMessageResponse};
use crate::{compute_engine::ComputeEngineNode, processor::factory_registry::ProcessorHandle};
use prost::Message;

impl CommandAction for ProcessorMessage {
    fn handle(
        &self,
        compute_engine: &mut ComputeEngineNode,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let payload_id = "processor_message".to_string();

        let processor_handle: Option<ProcessorHandle> =
            compute_engine.get_processor(self.processor_id.clone());

        match processor_handle {
            Some(processor_handle) => {
                let response = {
                    let mut processor = processor_handle.processor_mut();
                    match processor.handle_message(&self.payload) {
                        Ok(reply) => ProcessorMessageResponse {
                            payload: reply,
                            error: String::new(),
                        },
                        Err(err) => ProcessorMessageResponse {
                            payload: Vec::new(),
                            error: err,
                        },
                    }
                };

                (payload_id, response.encode_to_vec())
            }
            None => {
                errors.push(format!("Processor with id {} not found", self.processor_id));
                (payload_id, Vec::new())
            }
        }
    }
}
