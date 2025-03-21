use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};

use super::CommandAction;

use serde::Deserialize;
use serde_json::Value;

use crate::node::OperatorNode;
use crate::service::payloader;
use crate::pipeline::factory_registry::PipelineHandle;

#[derive(Deserialize)]
pub struct SerializePipelineCommand {
    pipeline_id: String
}

impl CommandAction for SerializePipelineCommand {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        reference_id: String,
    ) {
        let operator_guard = operator.lock().unwrap();
        let pipeline_handle: Option<PipelineHandle> = operator_guard.get_pipeline(self.pipeline_id.clone());

        let control_stream = operator_guard.get_control_stream();

        drop(operator_guard);

        let (serialized_state, factory_id_opt) = match pipeline_handle {
            Some(p) => {
                let pipeline = p.pipeline_mut();

                let serialized_state = pipeline.serialize_state().unwrap();

                let data_as_values = serialized_state.iter()
                    .map(|&byte| Value::Number(serde_json::Number::from(byte)))
                    .collect();

                let factory_id = pipeline.get_factory_id();
    
                (Value::Array(data_as_values), Some(factory_id))
            }
            None => (Value::Null, None)
        };

        // Create a data object that contains both components
        let mut data_object = serde_json::Map::new();
        data_object.insert("serialized_state".to_string(), serialized_state);
        
        if let Some(factory_id) = factory_id_opt {
            data_object.insert("factory_id".to_string(), Value::String(factory_id));
        }

        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));
        hashmap.insert("data".to_string(), Value::Object(data_object));
        
        let json_string = serde_json::to_string(&hashmap).unwrap();
        
        let payload = payloader::build_from_str(&"serialize_pipeline".to_string(), &json_string);
        control_stream.try_clone().unwrap().write_all(&payload).unwrap();
    }
}