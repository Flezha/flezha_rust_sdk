use std::collections::HashMap;

pub mod executor;
pub mod factory_registry;

extern crate erased_serde;

pub type ComputationError = String;
pub type InputError = String;
pub type ComputationId = String;
pub type PayloadMetadata = HashMap<String, String>;

#[derive(Debug)]
pub enum ComputationInput {
    Binary(Vec<u8>),
    Text(String),
    Custom(Box<dyn std::any::Any + Send + Sync>), // For client‐specific types
}

#[derive(Clone)]
pub enum ComputationRunner {
    ThreadPool,
    ThreadSpawn,
}

impl From<ComputationOutput> for ComputationInput {
    fn from(output: ComputationOutput) -> Self {
        match output {
            ComputationOutput::Binary(data) => ComputationInput::Binary(data),
            ComputationOutput::Text(text) => ComputationInput::Text(text),
            ComputationOutput::Custom(custom) => ComputationInput::Custom(custom),
        }
    }
}

#[derive(Debug)]
pub enum ComputationOutput {
    Binary(Vec<u8>),
    Text(String),
    Custom(Box<dyn std::any::Any + Send + Sync>), // For client‐specific types
}

#[derive(Debug)]
pub enum ComputationResult {
    None,
    Return(ComputationOutput),
    PipeToAgent(Vec<(ComputationOutput, PayloadMetadata)>),
    PipeToComputation(Vec<(ComputationId, ComputationOutput, PayloadMetadata)>),
}

pub trait Computation {
    // Execute the computation on the given input. The computation can mutate its internal state.
    fn run(&mut self, input: ComputationInput) -> Result<ComputationResult, ComputationError>;

    // Serialize the computation state.
    fn json_serialize_state(&self) -> Result<Vec<u8>, String>;

    // Deserialize and restore the computation state.
    fn json_deserialize_state(&mut self, data: &[u8]) -> Result<(), String>;

    // Get the factory ID of the computation.
    fn get_factory_id(&self) -> String;

    // Get computation state for downcasting and serialization
    fn get_state(&mut self) -> &mut dyn std::any::Any;

    // Get computation state as a serializable reference
    fn bin_serialize_state(&self) -> Box<dyn erased_serde::Serialize + '_>;

    // Optional method to perform actions after the computation is created or altered
    fn post_alter(&mut self);

    // Get the settings of the computation
    fn get_runner_type(&self) -> ComputationRunner;
}

/// Trait for transforming input data to generic type before it enters the computation steps
pub trait InputTransformer<Input> {
    fn transform_input(&self, input: ComputationInput) -> Result<Input, InputError>;
    fn clone_box(&self) -> Box<dyn InputTransformer<Input> + Send + Sync>;
}

/// Trait for transforming generic output data to ComputationResult after all computation steps complete
pub trait OutputTransformer<Output, State> {
    fn transform_output(&self, output: Output, state: &mut State) -> ComputationResult;
    fn clone_box(&self) -> Box<dyn OutputTransformer<Output, State> + Send + Sync>;
}

/// Default input transformer that converts ComputationInput to common types
pub struct DefaultInputTransformer;

impl InputTransformer<Vec<u8>> for DefaultInputTransformer {
    fn transform_input(&self, input: ComputationInput) -> Result<Vec<u8>, InputError> {
        match input {
            ComputationInput::Binary(data) => Ok(data),
            ComputationInput::Text(text) => Ok(text.into_bytes()),
            ComputationInput::Custom(_) => Err("Cannot convert custom to Vec<u8>".into()),
        }
    }

    fn clone_box(&self) -> Box<dyn InputTransformer<Vec<u8>> + Send + Sync> {
        Box::new(DefaultInputTransformer)
    }
}

impl InputTransformer<String> for DefaultInputTransformer {
    fn transform_input(&self, input: ComputationInput) -> Result<String, InputError> {
        match input {
            ComputationInput::Text(text) => Ok(text),
            ComputationInput::Binary(data) => String::from_utf8(data).map_err(|e| e.to_string()),
            ComputationInput::Custom(_) => Err("Cannot convert custom to String".into()),
        }
    }

    fn clone_box(&self) -> Box<dyn InputTransformer<String> + Send + Sync> {
        Box::new(DefaultInputTransformer)
    }
}

/// Default output transformer that converts common types to binary
pub struct DefaultOutputTransformer;

impl<State> OutputTransformer<String, State> for DefaultOutputTransformer {
    fn transform_output(&self, output: String, _state: &mut State) -> ComputationResult {
        ComputationResult::PipeToAgent(vec![(
            ComputationOutput::Binary(output.into_bytes()),
            HashMap::new(),
        )])
    }

    fn clone_box(&self) -> Box<dyn OutputTransformer<String, State> + Send + Sync> {
        Box::new(DefaultOutputTransformer)
    }
}

pub trait ComputationHandler<State, I, O> {
    fn process(&mut self, input: I, state: &mut State) -> Result<O, ComputationError>;

    // Optional method to perform actions after the step is created
    fn post_create(&mut self, _state: &mut State) {}
}

pub struct ComputationSettings<State, I, O> {
    input_transformer: Box<dyn InputTransformer<I> + Send + Sync>,
    output_transformer: Box<dyn OutputTransformer<O, State> + Send + Sync>,
    computation_runner: ComputationRunner,
}

impl<State, I, O> Clone for ComputationSettings<State, I, O> {
    fn clone(&self) -> Self {
        Self {
            input_transformer: self.input_transformer.clone_box(),
            output_transformer: self.output_transformer.clone_box(),
            computation_runner: self.computation_runner.clone(),
        }
    }
}

impl<State, I, O> ComputationSettings<State, I, O> {
    pub fn new(
        input_transformer: Box<dyn InputTransformer<I> + Send + Sync>,
        output_transformer: Box<dyn OutputTransformer<O, State> + Send + Sync>,
        computation_runner: ComputationRunner,
    ) -> Self {
        Self {
            input_transformer,
            output_transformer,
            computation_runner,
        }
    }

    pub fn get_input_transformer(&self) -> &dyn InputTransformer<I> {
        self.input_transformer.as_ref()
    }

    pub fn get_output_transformer(&self) -> &dyn OutputTransformer<O, State> {
        self.output_transformer.as_ref()
    }

    pub fn get_computation_runner(&self) -> &ComputationRunner {
        &self.computation_runner
    }
}

impl<State> ComputationSettings<State, String, String> {
    pub fn default() -> Self {
        Self {
            input_transformer: Box::new(DefaultInputTransformer),
            output_transformer: Box::new(DefaultOutputTransformer),
            computation_runner: ComputationRunner::ThreadPool,
        }
    }
}

/// A concrete computation implementation that can be used with any state type
pub struct ComputationWrapper<State, I, O>
where
    State: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    state: State,
    handler: Box<dyn ComputationHandler<State, I, O> + Send + Sync>,
    settings: ComputationSettings<State, I, O>,
    factory_id: String,
}

impl<State, I, O> Computation for ComputationWrapper<State, I, O>
where
    State: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    fn run(&mut self, input: ComputationInput) -> Result<ComputationResult, ComputationError> {
        // Convert ComputationInput to generic input type using input transformer
        let current_input = match self.settings.get_input_transformer().transform_input(input) {
            Ok(transformed) => transformed,
            Err(e) => return Err(e),
        };

        match self.handler.process(current_input, &mut self.state) {
            Ok(output) => {
                // Let the output transformer decide the result type
                let result = self
                    .settings
                    .get_output_transformer()
                    .transform_output(output, &mut self.state);

                Ok(result)
            }
            Err(err) => Err(format!("Computation error: {}", err)),
        }
    }

    fn json_serialize_state(&self) -> Result<Vec<u8>, String> {
        serde_json::to_vec(&self.state).map_err(|e| format!("Serialization error: {}", e))
    }

    fn json_deserialize_state(&mut self, data: &[u8]) -> Result<(), String> {
        self.state =
            serde_json::from_slice(data).map_err(|e| format!("Deserialization error: {}", e))?;
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
        self.handler.post_create(&mut self.state);
    }

    fn get_runner_type(&self) -> ComputationRunner {
        self.settings.get_computation_runner().clone()
    }
}

pub fn register_factory<State, I, O, F>(
    factory_id: String,
    settings: ComputationSettings<State, I, O>,
    initial_state: State,
    handler_generator: F,
) where
    State: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: Fn() -> Box<dyn ComputationHandler<State, I, O> + Send + Sync> + Send + Sync + 'static,
{
    use crate::computation::factory_registry::{register_computation_factory, ComputationFactory};

    let factory_id_clone = factory_id.clone();
    let settings_clone = settings.clone();

    let factory: ComputationFactory = Box::new(move |_computation_id: String| {
        let handler = handler_generator();
        let mut computation = ComputationWrapper {
            state: initial_state.clone(),
            handler: handler,
            settings: settings_clone.clone(),
            factory_id: factory_id_clone.clone(),
        };

        computation.post_alter();
        Box::new(computation) as Box<dyn Computation + Send + Sync>
    });

    register_computation_factory(factory_id, factory);
}
