use super::CommandAction;

use std::sync::{Arc, Mutex};
use serde_json::Value;
use serde::Deserialize;

use crate::node::OperatorNode;
use crate::pipeline::factory_registry::get_factory;


#[derive(Deserialize, Debug)]
pub struct AddPipelineCommand {
    pipeline_id: String,
    factory_id: String,
    init_values: Value,
}

impl CommandAction for AddPipelineCommand {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        errors: &mut Vec<String>
    ) -> (String, String) {
        let operator_guard = operator.lock().unwrap();
        let pipeline_id = self.pipeline_id.clone();

        // Make sure factory exist.
        match get_factory(self.factory_id.clone()) {
            Some(_) => {},
            None => errors.push(format!("Factory with id {} does not exist", self.factory_id.clone())),
        };

        let pipeline_opt = operator_guard.start_pipeline(
            self.factory_id.clone(),
            pipeline_id
        );

        if errors.len() == 0 {
            match pipeline_opt {
                Some(pipeline_handle) => {
                    if let Some(arr) = self.init_values.as_object() {
                        if !arr.is_empty() {                            
                            let default_values: Vec<u8> = serde_json::to_vec(&self.init_values).expect("Failed to serialize init values");
                            let mut pipeline = pipeline_handle.pipeline_mut();
        
                            pipeline.deserialize_state(&default_values)
                                .expect("Failed to deserialize state");
                        }
                    }
                    
                    return (
                        "start_pipeline".to_string(),
                        "Pipeline has been created".to_string()
                    )
                },
                None => errors.push(format!("Unable to create pipeline"))
            }
        }

        ("start_pipeline".to_string(), String::from(""))
    }
}

