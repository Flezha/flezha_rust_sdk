use std::collections::HashMap;

pub mod executor;
pub mod factory_registry;

extern crate erased_serde;

pub type ProcessorError = String;
pub type InputError = String;
pub type ProcessorId = String;
pub type PayloadMetadata = HashMap<String, String>;

#[derive(Debug)]
pub enum ProcessorInput {
    Binary(Vec<u8>),
    Text(String),
    Custom(Box<dyn std::any::Any + Send + Sync>), // For client‐specific types
}

#[derive(Clone)]
pub enum ProcessorRunner {
    ThreadPool,
    ThreadSpawn,
}

impl From<ProcessorOutput> for ProcessorInput {
    fn from(output: ProcessorOutput) -> Self {
        match output {
            ProcessorOutput::Binary(data) => ProcessorInput::Binary(data),
            ProcessorOutput::Text(text) => ProcessorInput::Text(text),
            ProcessorOutput::Custom(custom) => ProcessorInput::Custom(custom),
        }
    }
}

#[derive(Debug)]
pub enum ProcessorOutput {
    Binary(Vec<u8>),
    Text(String),
    Custom(Box<dyn std::any::Any + Send + Sync>), // For client‐specific types
}

#[derive(Debug)]
pub enum ProcessorResult {
    None,
    Return(ProcessorOutput),
    PipeToAgent(Vec<(ProcessorOutput, PayloadMetadata)>),
    PipeToProcessor(Vec<(ProcessorId, ProcessorOutput, PayloadMetadata)>),
}

pub trait Processor {
    // Execute the processor on the given input. The processor can mutate its internal state.
    fn run(&mut self, input: ProcessorInput) -> Result<ProcessorResult, ProcessorError>;

    // Serialize the processor state.
    fn json_serialize_state(&self) -> Result<Vec<u8>, String>;

    // Deserialize and restore the processor state.
    fn json_deserialize_state(&mut self, data: &[u8]) -> Result<(), String>;

    // Get the factory ID of the processor.
    fn get_factory_id(&self) -> String;

    // Get processor state for downcasting and serialization
    fn get_state(&mut self) -> &mut dyn std::any::Any;

    // Get processor state as a serializable reference
    fn bin_serialize_state(&self) -> Box<dyn erased_serde::Serialize + '_>;

    // Optional method to perform actions after the processor is created or altered
    fn post_alter(&mut self);

    // Get the settings of the processor
    fn get_runner_type(&self) -> ProcessorRunner;
}

/// Trait for transforming input data to generic type before it enters the processor steps
pub trait InputTransformer<Input> {
    fn transform_input(&self, input: ProcessorInput) -> Result<Input, InputError>;
    fn clone_box(&self) -> Box<dyn InputTransformer<Input> + Send + Sync>;
}

/// Trait for transforming generic output data to ProcessorResult after all processor steps complete
pub trait OutputTransformer<Output, State> {
    fn transform_output(&self, output: Output, state: &mut State) -> ProcessorResult;
    fn clone_box(&self) -> Box<dyn OutputTransformer<Output, State> + Send + Sync>;
}

/// Default input transformer that converts ProcessorInput to common types
pub struct DefaultInputTransformer;

impl InputTransformer<Vec<u8>> for DefaultInputTransformer {
    fn transform_input(&self, input: ProcessorInput) -> Result<Vec<u8>, InputError> {
        match input {
            ProcessorInput::Binary(data) => Ok(data),
            ProcessorInput::Text(text) => Ok(text.into_bytes()),
            ProcessorInput::Custom(_) => Err("Cannot convert custom to Vec<u8>".into()),
        }
    }

    fn clone_box(&self) -> Box<dyn InputTransformer<Vec<u8>> + Send + Sync> {
        Box::new(DefaultInputTransformer)
    }
}

impl InputTransformer<String> for DefaultInputTransformer {
    fn transform_input(&self, input: ProcessorInput) -> Result<String, InputError> {
        match input {
            ProcessorInput::Text(text) => Ok(text),
            ProcessorInput::Binary(data) => String::from_utf8(data).map_err(|e| e.to_string()),
            ProcessorInput::Custom(_) => Err("Cannot convert custom to String".into()),
        }
    }

    fn clone_box(&self) -> Box<dyn InputTransformer<String> + Send + Sync> {
        Box::new(DefaultInputTransformer)
    }
}

/// Default output transformer that converts common types to binary
pub struct DefaultOutputTransformer;

impl<State> OutputTransformer<String, State> for DefaultOutputTransformer {
    fn transform_output(&self, output: String, _state: &mut State) -> ProcessorResult {
        ProcessorResult::PipeToAgent(vec![(
            ProcessorOutput::Binary(output.into_bytes()),
            HashMap::new(),
        )])
    }

    fn clone_box(&self) -> Box<dyn OutputTransformer<String, State> + Send + Sync> {
        Box::new(DefaultOutputTransformer)
    }
}

pub trait ProcessorHandler<State, I, O> {
    fn process(&mut self, input: I, state: &mut State) -> Result<O, ProcessorError>;

    // Optional method to perform actions after the step is created
    fn post_create(&mut self, _state: &mut State) {}
}

pub struct ProcessorSettings<State, I, O> {
    input_transformer: Box<dyn InputTransformer<I> + Send + Sync>,
    output_transformer: Box<dyn OutputTransformer<O, State> + Send + Sync>,
    processor_runner: ProcessorRunner,
}

impl<State, I, O> Clone for ProcessorSettings<State, I, O> {
    fn clone(&self) -> Self {
        Self {
            input_transformer: self.input_transformer.clone_box(),
            output_transformer: self.output_transformer.clone_box(),
            processor_runner: self.processor_runner.clone(),
        }
    }
}

impl<State, I, O> ProcessorSettings<State, I, O> {
    pub fn new(
        input_transformer: Box<dyn InputTransformer<I> + Send + Sync>,
        output_transformer: Box<dyn OutputTransformer<O, State> + Send + Sync>,
        processor_runner: ProcessorRunner,
    ) -> Self {
        Self {
            input_transformer,
            output_transformer,
            processor_runner,
        }
    }

    pub fn get_input_transformer(&self) -> &dyn InputTransformer<I> {
        self.input_transformer.as_ref()
    }

    pub fn get_output_transformer(&self) -> &dyn OutputTransformer<O, State> {
        self.output_transformer.as_ref()
    }

    pub fn get_processor_runner(&self) -> &ProcessorRunner {
        &self.processor_runner
    }
}

impl<State> ProcessorSettings<State, String, String> {
    pub fn default() -> Self {
        Self {
            input_transformer: Box::new(DefaultInputTransformer),
            output_transformer: Box::new(DefaultOutputTransformer),
            processor_runner: ProcessorRunner::ThreadPool,
        }
    }
}

/// A concrete processor implementation that can be used with any state type
pub struct ProcessorWrapper<State, I, O>
where
    State: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    state: State,
    handler: Box<dyn ProcessorHandler<State, I, O> + Send + Sync>,
    settings: ProcessorSettings<State, I, O>,
    factory_id: String,
}

impl<State, I, O> Processor for ProcessorWrapper<State, I, O>
where
    State: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    fn run(&mut self, input: ProcessorInput) -> Result<ProcessorResult, ProcessorError> {
        // Convert ProcessorInput to generic input type using input transformer
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
            Err(err) => Err(format!("Processor error: {}", err)),
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

    fn get_runner_type(&self) -> ProcessorRunner {
        self.settings.get_processor_runner().clone()
    }
}

pub fn register_factory<State, I, O, F>(
    factory_id: String,
    settings: ProcessorSettings<State, I, O>,
    initial_state: State,
    handler_generator: F,
    description: Option<String>,
) where
    State: serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
    F: Fn() -> Box<dyn ProcessorHandler<State, I, O> + Send + Sync> + Send + Sync + 'static,
{
    use crate::processor::factory_registry::{
        register_factory_description, register_processor_factory, ProcessorFactory,
    };

    let factory_id_clone = factory_id.clone();
    let settings_clone = settings.clone();

    let factory: ProcessorFactory = Box::new(move |_processor_id: String| {
        let handler = handler_generator();
        let mut processor = ProcessorWrapper {
            state: initial_state.clone(),
            handler: handler,
            settings: settings_clone.clone(),
            factory_id: factory_id_clone.clone(),
        };

        processor.post_alter();
        Box::new(processor) as Box<dyn Processor + Send + Sync>
    });

    register_factory_description(factory_id.clone(), description);
    register_processor_factory(factory_id, factory);
}
