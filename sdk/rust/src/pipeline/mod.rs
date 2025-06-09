pub mod factory_registry;
pub mod runner;

use std::io;
extern crate erased_serde;

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
    // Execute the pipeline on the given input. The pipeline can mutate its internal state.
    fn run(&mut self, input: PipelineInput) -> PipelineResult;
    
    // Serialize the pipeline state.
    fn serialize_state(&self) -> io::Result<Vec<u8>>;
    
    // Deserialize and restore the pipeline state.
    fn deserialize_state(&mut self, data: &[u8]) -> io::Result<()>;

    // Get the factory ID of the pipeline.
    fn get_factory_id(&self) -> String;

    // Get pipeline state as a serializable reference
    fn get_state(&self) -> Box<dyn erased_serde::Serialize + '_>;
}

pub trait PipelineStep<State> {
    fn process(&mut self, input: PipelineInput, state: &mut State) -> io::Result<PipelineOutput>;
        
    // Optional method to perform actions after the step is created    
    fn post_create(&mut self, _state: &mut State) {}
}

#[macro_export]
macro_rules! define_pipeline_factory {
    (
        factory_id: $factory_id:expr,
        result_type: $result_type:expr,
        state: $state:expr,
        steps: [ $( $step_object:expr ),+ $(,)? ]
    ) => {{
        use $crate::pipeline::{Pipeline, PipelineInput, PipelineOutput, PipelineResult};
        use $crate::pipeline::factory_registry::{register_pipeline_factory, PipelineFactory};
        use std::io;
        use serde::{Serialize, Deserialize};
        use erased_serde;

        let factory_id_string = $factory_id.clone();
        let factory_id_for_registration = factory_id_string.clone();

        let factory: PipelineFactory = Box::new(move |_pipeline_id: String| {
            // The pipeline is defined with a generic state type.
            // This enforces that the provided state expression has the required traits.
            struct ConcretePipeline<State: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static> {
                state: State,
                steps: Vec<Box<dyn PipelineStep<State> + Send + Sync>>,
                result_type: $crate::pipeline::PipelineResultType,
                factory_id: String,
            }

            impl<State: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static> Pipeline for ConcretePipeline<State> {
                fn run(&mut self, input: PipelineInput) -> PipelineResult {
                    let mut current_input = input;
                    let mut final_output = PipelineOutput::Binary(vec![]);
                    
                    for step in &mut self.steps {
                        match step.process(current_input, &mut self.state) {
                            Ok(output) => {
                                final_output = output;
                                current_input = match &final_output {
                                    PipelineOutput::Binary(data) => PipelineInput::Binary(data.clone()),
                                    PipelineOutput::Text(text) => PipelineInput::Text(text.clone()),
                                    PipelineOutput::Custom(_) => return PipelineResult::None,
                                };
                            },
                            Err(_) => return PipelineResult::None,
                        }
                    }

                    match self.result_type {
                        $crate::pipeline::PipelineResultType::None => PipelineResult::None,
                        $crate::pipeline::PipelineResultType::Forward => PipelineResult::Forward(final_output),
                    }
                }
                
                fn serialize_state(&self) -> io::Result<Vec<u8>> {
                    serde_json::to_vec(&self.state)
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                }
                
                fn deserialize_state(&mut self, data: &[u8]) -> io::Result<()> {
                    self.state = serde_json::from_slice(data)
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    Ok(())
                }

                fn get_factory_id(&self) -> String {
                    self.factory_id.clone()
                }

                fn get_state(&self) -> Box<dyn erased_serde::Serialize + '_> {
                    Box::new(&self.state)
                }
            }

            // Create the pipeline with proper state initialization
            let mut state = $state;
            let mut steps: Vec<Box<dyn PipelineStep<_> + Send + Sync>> = vec![ $( Box::new($step_object) ),+ ];
            
            // Call post_create on each step
            for step in &mut steps {
                step.post_create(&mut state);
            }

            let pipeline = ConcretePipeline {
                state,
                steps,
                result_type: $result_type,
                factory_id: factory_id_string.clone(),
            };

            Box::new(pipeline)
        });

        register_pipeline_factory(factory_id_for_registration, factory);
    }};
}
