pub mod factory_registry;
pub mod runner;

use std::io;

extern crate erased_serde;

pub enum PipelineInput {
    Binary(Vec<u8>),
    Text(String),
    Custom(Box<dyn std::any::Any + Send + Sync>), // For client‐specific types
}

pub enum PipelineOutput {
    Binary(Vec<u8>),
    Text(String),
    Custom(Box<dyn std::any::Any + Send + Sync>), // For client‐specific types
}

pub type VirtualChannel = u8;

pub enum PipelineResult {
    None,
    Forward(Vec<(PipelineOutput, VirtualChannel)>),
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

/// Trait for transforming input data to generic type before it enters the pipeline steps
pub trait InputTransformer<T> {
    fn transform_input(&self, input: PipelineInput) -> io::Result<T>;
    fn clone_box(&self) -> Box<dyn InputTransformer<T> + Send + Sync>;
}

/// Trait for transforming generic output data to PipelineResult after all pipeline steps complete
pub trait OutputTransformer<T, State> {
    fn transform_output(&self, output: T, state: &State) -> PipelineResult;
    fn clone_box(&self) -> Box<dyn OutputTransformer<T, State> + Send + Sync>;
}

/// Default input transformer that converts PipelineInput to common types
pub struct DefaultInputTransformer;

impl InputTransformer<Vec<u8>> for DefaultInputTransformer {
    fn transform_input(&self, input: PipelineInput) -> io::Result<Vec<u8>> {
        match input {
            PipelineInput::Binary(data) => Ok(data),
            PipelineInput::Text(text) => Ok(text.into_bytes()),
            PipelineInput::Custom(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Cannot convert custom to Vec<u8>",
            )),
        }
    }

    fn clone_box(&self) -> Box<dyn InputTransformer<Vec<u8>> + Send + Sync> {
        Box::new(DefaultInputTransformer)
    }
}

impl InputTransformer<String> for DefaultInputTransformer {
    fn transform_input(&self, input: PipelineInput) -> io::Result<String> {
        match input {
            PipelineInput::Text(text) => Ok(text),
            PipelineInput::Binary(data) => {
                String::from_utf8(data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
            }
            PipelineInput::Custom(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Cannot convert custom to String",
            )),
        }
    }

    fn clone_box(&self) -> Box<dyn InputTransformer<String> + Send + Sync> {
        Box::new(DefaultInputTransformer)
    }
}

/// Default output transformer that converts common types to binary
pub struct DefaultOutputTransformer;

impl<State> OutputTransformer<String, State> for DefaultOutputTransformer {
    fn transform_output(&self, output: String, _state: &State) -> PipelineResult {
        PipelineResult::Forward(vec![(PipelineOutput::Binary(output.into_bytes()), 1)])
    }

    fn clone_box(&self) -> Box<dyn OutputTransformer<String, State> + Send + Sync> {
        Box::new(DefaultOutputTransformer)
    }
}

pub trait PipelineStep<State, T> {
    fn process(&mut self, input: T, state: &mut State) -> io::Result<T>;

    // Optional method to perform actions after the step is created
    fn post_create(&mut self, _state: &mut State) {}
}

pub struct PipelineSettings<T, State> {
    input_transformer: Box<dyn InputTransformer<T> + Send + Sync>,
    output_transformer: Box<dyn OutputTransformer<T, State> + Send + Sync>,
}

impl<T, State> Clone for PipelineSettings<T, State> {
    fn clone(&self) -> Self {
        Self {
            input_transformer: self.input_transformer.clone_box(),
            output_transformer: self.output_transformer.clone_box(),
        }
    }
}

impl<T, State> PipelineSettings<T, State> {
    pub fn new(
        input_transformer: Box<dyn InputTransformer<T> + Send + Sync>,
        output_transformer: Box<dyn OutputTransformer<T, State> + Send + Sync>,
    ) -> Self {
        Self {
            input_transformer,
            output_transformer,
        }
    }

    pub fn get_input_transformer(&self) -> &dyn InputTransformer<T> {
        self.input_transformer.as_ref()
    }

    pub fn get_output_transformer(&self) -> &dyn OutputTransformer<T, State> {
        self.output_transformer.as_ref()
    }
}

impl<State> PipelineSettings<String, State> {
    pub fn default() -> Self {
        Self {
            input_transformer: Box::new(DefaultInputTransformer),
            output_transformer: Box::new(DefaultOutputTransformer),
        }
    }
}

/// A concrete pipeline implementation that can be used with any state type
pub struct ConcretePipeline<State, T>
where
    State: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    T: Send + Sync + 'static,
{
    state: State,
    steps: Vec<Box<dyn PipelineStep<State, T> + Send + Sync>>,
    settings: PipelineSettings<T, State>,
    factory_id: String,
}

impl<State, T> Pipeline for ConcretePipeline<State, T>
where
    State: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    T: Send + Sync + 'static,
{
    fn run(&mut self, input: PipelineInput) -> PipelineResult {
        // Convert PipelineInput to generic input type using input transformer
        let mut current_input = match self.settings.get_input_transformer().transform_input(input) {
            Ok(transformed) => transformed,
            Err(_) => return PipelineResult::None,
        };

        for step in &mut self.steps {
            match step.process(current_input, &mut self.state) {
                Ok(output) => {
                    current_input = output;
                }
                Err(_) => return PipelineResult::None,
            }
        }

        // Let the output transformer decide the result type
        self.settings
            .get_output_transformer()
            .transform_output(current_input, &self.state)
    }

    fn json_serialize_state(&self) -> io::Result<Vec<u8>> {
        serde_json::to_vec(&self.state).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    fn json_deserialize_state(&mut self, data: &[u8]) -> io::Result<()> {
        self.state =
            serde_json::from_slice(data).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
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

pub fn create_pipeline_factory<State, T, F>(
    factory_id: String,
    settings: PipelineSettings<T, State>,
    initial_state: State,
    step_factory: F,
) where
    State: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
    T: Send + Sync + 'static,
    F: Fn() -> Vec<Box<dyn PipelineStep<State, T> + Send + Sync>> + Send + Sync + 'static,
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
