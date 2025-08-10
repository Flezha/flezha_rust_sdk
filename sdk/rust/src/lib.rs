mod command;
mod compute_engine;
pub mod processor;
mod service;
mod utility;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use crossbeam::channel::{Receiver, Sender};

pub use processor::{
    register_factory, ProcessorHandler, ProcessorInput, ProcessorOutput, ProcessorResult,
    ProcessorSettings,
};

use crate::{
    compute_engine::{ClientResponseMessage, ComputeEngineMessage},
    processor::factory_registry::ProcessorBox,
};

pub mod pb {
    #[cfg(not(rust_analyzer))]
    include!("generated/compute_engine_commands.rs");
    #[cfg(rust_analyzer)]
    include!("generated/compute_engine_commands.rs");
}
pub struct Flezha {
    id: String,
    compute_engine_tx: Arc<Mutex<crossbeam::channel::Sender<ComputeEngineMessage>>>,
    client_rx: Arc<Mutex<crossbeam::channel::Receiver<ClientResponseMessage>>>,
    _compute_engine_thread: Option<std::thread::JoinHandle<()>>,
}

impl Flezha {
    fn new(id: String) -> Flezha {
        let (otx, orx): (Sender<ComputeEngineMessage>, Receiver<ComputeEngineMessage>) =
            crossbeam::channel::unbounded();
        let (ctx, irx): (
            Sender<ClientResponseMessage>,
            Receiver<ClientResponseMessage>,
        ) = crossbeam::channel::unbounded();
        let compute_engine_thread = compute_engine::start(id.clone(), (otx.clone(), orx), ctx);

        // Make sure the namespace is passed to the agent and confirmed
        match irx
            .recv()
            .expect("Failed to receive namespace confirmation")
        {
            ClientResponseMessage::ComputeEngineIdConfirmation => {}
            _ => panic!("Unexpected message type received"),
        }

        Flezha {
            id: id.clone(),
            compute_engine_tx: Arc::new(Mutex::new(otx)),
            client_rx: Arc::new(Mutex::new(irx)),
            _compute_engine_thread: Some(compute_engine_thread),
        }
    }

    pub fn get_id(&self) -> &str {
        &self.id
    }

    pub fn get_processors(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<ProcessorBox>>>>> {
        self.compute_engine_tx
            .lock()
            .unwrap()
            .send(ComputeEngineMessage::ReturnProcessors)
            .expect("Failed to send ReturnProcessors message");

        // Wait for the response from the compute_engine
        let response = self
            .client_rx
            .lock()
            .unwrap()
            .recv()
            .expect("Failed to receive processors");

        match response {
            ClientResponseMessage::ReturnProcessors(processors) => processors,
            _ => panic!("Unexpected message type received"),
        }
    }

    // TODO: add api functions to get information and execute processors

    // TODO: add those public API methods to the Flezha struct
    // pub fn run(
    //     &mut self,
    //     processor_id: String,
    //     input: ProcessorInput,
    // ) -> Result<ProcessorResult, ProcessorError> {
    //     runner::exec(&processor_id, &self.processors, input)
    // }

    // pub fn pipe_to_agent(
    //     &mut self,
    //     processor_id: String,
    //     result: ProcessorOutput,
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

    //     runner::pipe_to_agent(&mut io_stream, &processor_id, &result, metadata);
    // }
}

pub fn new(namespace: String) -> Flezha {
    Flezha::new(namespace)
}
