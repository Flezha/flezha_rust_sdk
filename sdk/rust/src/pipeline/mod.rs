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

#[derive(Clone, Copy)]
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
    fn json_serialize_state(&self) -> io::Result<Vec<u8>>;
    
    // Deserialize and restore the pipeline state.
    fn json_deserialize_state(&mut self, data: &[u8]) -> io::Result<()>;

    // Get the factory ID of the pipeline.
    fn get_factory_id(&self) -> String;

    // Get pipeline state for downcasting and serialization
    fn get_state(&mut self) -> &mut dyn std::any::Any;

    // Get pipeline state as a serializable reference
    fn bin_serialize_state(&self) -> Box<dyn erased_serde::Serialize + '_>;

    fn post_alter(&mut self);
}

/// Trait for transforming input data before it enters the pipeline steps
pub trait InputTransformer {
    fn transform_input(&self, input: PipelineInput) -> io::Result<PipelineInput>;
    fn clone_box(&self) -> Box<dyn InputTransformer + Send + Sync>;
}

/// Trait for transforming output data after all pipeline steps complete
pub trait OutputTransformer {
    fn transform_output(&self, output: PipelineOutput) -> io::Result<PipelineOutput>;
    fn clone_box(&self) -> Box<dyn OutputTransformer + Send + Sync>;
}

/// Default input transformer that passes data through unchanged
pub struct DefaultInputTransformer;

impl InputTransformer for DefaultInputTransformer {
    fn transform_input(&self, input: PipelineInput) -> io::Result<PipelineInput> {
        Ok(input)
    }
    
    fn clone_box(&self) -> Box<dyn InputTransformer + Send + Sync> {
        Box::new(DefaultInputTransformer)
    }
}

/// Default output transformer that passes data through unchanged
pub struct DefaultOutputTransformer;

impl OutputTransformer for DefaultOutputTransformer {
    fn transform_output(&self, output: PipelineOutput) -> io::Result<PipelineOutput> {
        Ok(output)
    }
    
    fn clone_box(&self) -> Box<dyn OutputTransformer + Send + Sync> {
        Box::new(DefaultOutputTransformer)
    }
}


pub trait PipelineStep<State> {
    fn process(&mut self, input: PipelineInput, state: &mut State) -> io::Result<PipelineOutput>;
        
    // Optional method to perform actions after the step is created    
    fn post_create(&mut self, _state: &mut State) {}
}

pub struct PipelineSettings {
    result_type: PipelineResultType,
    input_transformer: Box<dyn InputTransformer + Send + Sync>,
    output_transformer: Box<dyn OutputTransformer + Send + Sync>,
}

impl Clone for PipelineSettings {
    fn clone(&self) -> Self {
        Self {
            result_type: self.result_type,
            input_transformer: self.input_transformer.clone_box(),
            output_transformer: self.output_transformer.clone_box(),
        }
    }
}

impl PipelineSettings {
    pub fn new(result_type: PipelineResultType, input_transformer: Box<dyn InputTransformer + Send + Sync>, output_transformer: Box<dyn OutputTransformer + Send + Sync>) -> Self {
        Self {
            result_type,
            input_transformer,
            output_transformer,
        }
    }

    pub fn default() -> Self {
        Self {
            result_type: PipelineResultType::Forward,
            input_transformer: Box::new(DefaultInputTransformer),
            output_transformer: Box::new(DefaultOutputTransformer),
        }
    }

    pub fn get_result_type(&self) -> PipelineResultType {
        self.result_type
    }
}

/// A concrete pipeline implementation that can be used with any state type
pub struct ConcretePipeline<State: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static> {
    state: State,
    steps: Vec<Box<dyn PipelineStep<State> + Send + Sync>>,
    settings: PipelineSettings,
    factory_id: String,
}

impl<State: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static> Pipeline for ConcretePipeline<State> {
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

        match self.settings.get_result_type() {
            PipelineResultType::None => PipelineResult::None,
            PipelineResultType::Forward => PipelineResult::Forward(final_output),
        }
    }
    
    fn json_serialize_state(&self) -> io::Result<Vec<u8>> {
        serde_json::to_vec(&self.state)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
    
    fn json_deserialize_state(&mut self, data: &[u8]) -> io::Result<()> {
        self.state = serde_json::from_slice(data)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(())
    }

    fn get_factory_id(&self) -> String {
        self.factory_id.clone()
    }

    fn get_state(&mut self) -> &mut dyn std::any::Any {
        &mut self.state
    }

    fn bin_serialize_state(&self) -> Box<dyn erased_serde::Serialize + '_> {
        Box::new(&self.state)
    }

    fn post_alter(&mut self) {
        for step in &mut self.steps {
            step.post_create(&mut self.state);
        }
    }
}

/// Simple function to create and register a pipeline factory
/// Use a factory function for creating steps since steps can't be cloned
pub fn create_pipeline_factory<State, F>(
    factory_id: String,
    settings: PipelineSettings,
    initial_state: State,
    step_factory: F,
) where
    State: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn() -> Vec<Box<dyn PipelineStep<State> + Send + Sync>> + Send + Sync + 'static,
{
    use crate::pipeline::factory_registry::{register_pipeline_factory, PipelineFactory};
    
    let factory_id_clone = factory_id.clone();
    let settings_clone = settings.clone();

    let factory: PipelineFactory = Box::new(move |_pipeline_id: String| {
        let steps = step_factory();
        let mut pipeline = ConcretePipeline {
            state: initial_state.clone(),
            steps,
            settings: settings_clone.clone(),
            factory_id: factory_id_clone.clone(),
        };
        
        pipeline.post_alter();
        Box::new(pipeline) as Box<dyn Pipeline + Send + Sync>
    });

    register_pipeline_factory(factory_id, factory);
}
