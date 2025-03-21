pub mod factory_registry;
pub mod runner;

use std::io;

pub enum PipelineInput {
    Binary(Vec<u8>),
    Text(String),
}

pub enum PipelineOutput {
    Binary(Vec<u8>),
    Text(String),
    Custom(Box<dyn std::any::Any + Send>), // For client‐specific types
}

pub enum PipelineResult {
    None,
    Forward(PipelineOutput),
}

pub enum PipelineResultType {
    None,
    Forward,
}

// Add conversion from PipelineInput to PipelineOutput
impl From<PipelineInput> for PipelineOutput {
    fn from(input: PipelineInput) -> Self {
        match input {
            PipelineInput::Binary(data) => PipelineOutput::Binary(data),
            PipelineInput::Text(text) => PipelineOutput::Text(text),
        }
    }
}

pub trait Pipeline {
    /// Execute the pipeline on the given input. The pipeline can mutate its internal state.
    fn run(&mut self, input: PipelineInput) -> PipelineResult;
    
    /// Serialize the pipeline state.
    fn serialize_state(&self) -> io::Result<Vec<u8>>;
    
    /// Deserialize and restore the pipeline state.
    fn deserialize_state(&mut self, data: &[u8]) -> io::Result<()>;

    /// Get the factory ID of the pipeline.
    fn get_factory_id(&self) -> String;
}

#[macro_export]
macro_rules! define_pipeline_factory {
    (
        factory_id: $factory_id:expr,
        result_type: $result_type:expr,
        state: $State:ty,
        steps: [ $( $step_fn:expr ),+ $(,)? ]
    ) => {{
        use $crate::pipeline::{Pipeline, PipelineInput, PipelineOutput, PipelineResult};
        use $crate::pipeline::factory_registry::{register_pipeline_factory, PipelineFactory};
        use std::io;
        
        let factory_id_string = $factory_id.clone();
        let factory_id_for_registration = factory_id_string.clone(); // Create another clone for registration
        
        let factory: PipelineFactory = Box::new(move |_pipeline_id: String| {
            struct ConcretePipeline {
                state: $State,
                steps: Vec<Box<dyn FnMut(PipelineInput, &mut $State) -> io::Result<PipelineOutput> + Send + Sync>>,
                result_type: $crate::pipeline::PipelineResultType,
                factory_id: String,
            }
            
            impl Pipeline for ConcretePipeline {
                fn run(&mut self, input: PipelineInput) -> PipelineResult {
                    // Each step takes the input and state
                    let mut current_input = input;
                    let mut final_output = PipelineOutput::Binary(vec![]);
                    
                    for step in &mut self.steps {
                        // Execute the current step
                        match step(current_input, &mut self.state) {
                            Ok(output) => {
                                // Save the output for potential return
                                final_output = output;
                                
                                // Convert output back to input for the next step
                                current_input = match &final_output {
                                    PipelineOutput::Binary(data) => PipelineInput::Binary(data.clone()),
                                    PipelineOutput::Text(text) => PipelineInput::Text(text.clone()),
                                    PipelineOutput::Custom(_) => {
                                        // Handle Custom type properly
                                        return PipelineResult::None;
                                    }
                                };
                            },
                            Err(_) => {
                                // Handle errors by returning None
                                return PipelineResult::None;
                            }
                        }
                    }

                    match self.result_type {
                        $crate::pipeline::PipelineResultType::None => PipelineResult::None,
                        $crate::pipeline::PipelineResultType::Forward => PipelineResult::Forward(final_output),
                    }
                }
                
                fn serialize_state(&self) -> io::Result<Vec<u8>> {
                    serde_json::to_vec(&self.state).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                }
                
                fn deserialize_state(&mut self, data: &[u8]) -> io::Result<()> {
                    self.state = serde_json::from_slice(data).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    Ok(())
                }

                fn get_factory_id(&self) -> String {
                    self.factory_id.clone()
                }
            }
            
            let pipeline = ConcretePipeline {
                state: <$State as Default>::default(),
                steps: vec![ $( Box::new($step_fn) ),+ ],
                result_type: $result_type,
                factory_id: factory_id_string.clone(),
            };
            
            // Return the created pipeline
            Box::new(pipeline)
        });
        
        // Register this factory using the separate clone
        register_pipeline_factory(factory_id_for_registration, factory);
    }};
}
