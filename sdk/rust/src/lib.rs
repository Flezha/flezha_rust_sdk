mod command;
pub mod computation;
mod operator;
mod service;
mod utility;

use std::sync::{Arc, Mutex};

use crossbeam::channel::{Receiver, Sender};

pub use computation::{
    register_factory, ComputationHandler, ComputationInput, ComputationOutput, ComputationResult,
    ComputationSettings,
};

use crate::operator::{InitializationMessage, OperatorMessage};

pub mod pb {
    #[cfg(not(rust_analyzer))]
    include!("generated/operator_commands.rs");
    #[cfg(rust_analyzer)]
    include!("generated/operator_commands.rs");
}
pub struct Flezha {
    id: String,
    _operator_channel: Arc<Mutex<crossbeam::channel::Sender<OperatorMessage>>>,
    _operator_thread: Option<std::thread::JoinHandle<()>>,
}

impl Flezha {
    fn new(id: String) -> Flezha {
        let (ctx, crx): (Sender<OperatorMessage>, Receiver<OperatorMessage>) =
            crossbeam::channel::unbounded();
        let (itx, irx): (
            Sender<InitializationMessage>,
            Receiver<InitializationMessage>,
        ) = crossbeam::channel::unbounded();
        let operator_thread = operator::start(id.clone(), (ctx.clone(), crx), itx);

        // Make sure the namespace is passed to the agent and confirmed
        match irx
            .recv()
            .expect("Failed to receive namespace confirmation")
        {
            InitializationMessage::OperatorIdConfirmation => {}
        }

        Flezha {
            id: id.clone(),
            _operator_channel: Arc::new(Mutex::new(ctx)),
            _operator_thread: Some(operator_thread),
        }
    }

    pub fn get_id(&self) -> &str {
        &self.id
    }

    // TODO: add api functions to get information and execute computations

    // TODO: add those public API methods to the Flezha struct
    // pub fn run(
    //     &mut self,
    //     computation_id: String,
    //     input: ComputationInput,
    // ) -> Result<ComputationResult, ComputationError> {
    //     runner::exec(&computation_id, &self.computations, input)
    // }

    // pub fn pipe_to_agent(
    //     &mut self,
    //     computation_id: String,
    //     result: ComputationOutput,
    //     metadata: PayloadMetadata,
    // ) {
    //     let mut io_stream = self
    //         .shared_io_streams
    //         .write()
    //         .unwrap()
    //         .values()
    //         .next()
    //         .unwrap()
    //         .try_clone()
    //         .unwrap();

    //     runner::pipe_to_agent(&mut io_stream, &computation_id, &result, metadata);
    // }
}

pub fn new(group_id: String) -> Flezha {
    Flezha::new(group_id)
}
