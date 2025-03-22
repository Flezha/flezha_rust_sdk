use super::CommandAction;

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use serde_json::Value;
use serde::Deserialize;

use crate::node::OperatorNode;
use crate::pipeline::factory_registry::get_factory;
use crate::service::payloader;


#[derive(Deserialize, Debug)]
pub struct AddPipelineCommand {
    pipeline_id: String,
    factory_id: String,
    variables: Value,
}

impl CommandAction for AddPipelineCommand {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        reference_id: String,
    ) {
        println!("AddPipelineCommand: {:?}", self);

        let operator_guard = operator.lock().unwrap();
        let pipeline_id = self.pipeline_id.clone();


        let mut errors: Vec<String> = vec![];
        let mut status: &str = "success";

        // Make sure factory exist.
        match get_factory(self.factory_id.clone()) {
            Some(_) => {},
            None => errors.push(format!("Factory with id {} does not exist", self.factory_id.clone())),
        };

        let pipeline_opt = operator_guard.add_pipeline(
            self.factory_id.clone(), 
            pipeline_id
        );

        if errors.len() == 0 {
            match pipeline_opt {
                Some(_pipeline_handle) => {
                    let _variables = value_to_hashmap(&self.variables).unwrap();
    
                    // TODO: Add later
                    // pipeline.update_pipeline(&variables);
                },
                None => errors.push(format!("Unable to create pipeline"))
            }
        }

        if errors.len() > 0 {
            status = "error";
        }
                        
        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));
        hashmap.insert("status".to_string(), Value::String(status.to_string()));
        hashmap.insert("errors".to_string(), Value::Array(errors.into_iter().map(Value::String).collect()));

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_from_str(&"add_pipeline".to_string(), &json_string);

        operator_guard.get_control_stream().try_clone().unwrap().write_all(&payload).unwrap();
    }
}

fn value_to_hashmap(value: &Value) -> Option<HashMap<String, Value>> {
    value.as_object().map(|map| {
        map.iter()
           .map(|(k, v)| (k.clone(), v.clone()))
           .collect()
    })
}
