mod command;
pub mod computation;
mod operator;
mod service;
mod utility;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use crossbeam::channel::{Receiver, Sender};

pub use computation::{
    register_factory, ComputationHandler, ComputationInput, ComputationOutput, ComputationResult,
    ComputationSettings,
};

use crate::{
    computation::factory_registry::ComputationBox,
    operator::{ClientResponseMessage, OperatorMessage},
};

pub mod pb {
    #[cfg(not(rust_analyzer))]
    include!("generated/operator_commands.rs");
    #[cfg(rust_analyzer)]
    include!("generated/operator_commands.rs");
}
pub struct Flezha {
    id: String,
    operator_tx: Arc<Mutex<crossbeam::channel::Sender<OperatorMessage>>>,
    client_rx: Arc<Mutex<crossbeam::channel::Receiver<ClientResponseMessage>>>,
    _operator_thread: Option<std::thread::JoinHandle<()>>,
}

impl Flezha {
    fn new(id: String) -> Flezha {
        let (otx, orx): (Sender<OperatorMessage>, Receiver<OperatorMessage>) =
            crossbeam::channel::unbounded();
        let (ctx, irx): (
            Sender<ClientResponseMessage>,
            Receiver<ClientResponseMessage>,
        ) = crossbeam::channel::unbounded();
        let operator_thread = operator::start(id.clone(), (otx.clone(), orx), ctx);

        // Make sure the namespace is passed to the agent and confirmed
        match irx
            .recv()
            .expect("Failed to receive namespace confirmation")
        {
            ClientResponseMessage::OperatorIdConfirmation => {}
            _ => panic!("Unexpected message type received"),
        }

        Flezha {
            id: id.clone(),
            operator_tx: Arc::new(Mutex::new(otx)),
            client_rx: Arc::new(Mutex::new(irx)),
            _operator_thread: Some(operator_thread),
        }
    }

    pub fn get_id(&self) -> &str {
        &self.id
    }

    pub fn get_computations(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<ComputationBox>>>>> {
        self.operator_tx
            .lock()
            .unwrap()
            .send(OperatorMessage::ReturnComputations)
            .expect("Failed to send ReturnComputations message");

        // Wait for the response from the operator
        let response = self
            .client_rx
            .lock()
            .unwrap()
            .recv()
            .expect("Failed to receive computations");

        match response {
            ClientResponseMessage::ReturnComputations(computations) => computations,
            _ => panic!("Unexpected message type received"),
        }
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
