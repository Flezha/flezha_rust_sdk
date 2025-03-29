use std::sync::{Arc, Mutex};

use super::CommandAction;

use serde::Deserialize;
use serde_json::Value;

use crate::node::OperatorNode;
use crate::pipeline::factory_registry::PipelineHandle;

#[derive(Deserialize)]
pub struct MigrationSerializationCommand {
    pipeline_id: String
}

impl CommandAction for MigrationSerializationCommand {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        _errors: &mut Vec<String>
    ) -> (String, String) {       
        let operator_guard = operator.lock().unwrap();
        let pipeline_handle: Option<PipelineHandle> = operator_guard.get_pipeline(self.pipeline_id.clone());

        // TODO: Implement error handling.

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

        // Remove the pipeline from the operator
        operator_guard.rem_pipeline(self.pipeline_id.clone());

        drop(operator_guard);

        (
            "migration_serialization".to_string(),
            serde_json::to_string(&Value::Object(data_object)).unwrap()
        )
    }
}