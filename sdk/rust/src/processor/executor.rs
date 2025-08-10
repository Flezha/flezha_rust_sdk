use std::io::Write;
use std::sync::{Arc, Mutex, RwLock};
use std::{collections::HashMap, os::unix::net::UnixStream};

use crate::processor::factory_registry::ProcessorBox;
use crate::processor::{
    PayloadMetadata, ProcessorError, ProcessorId, ProcessorInput, ProcessorOutput, ProcessorResult,
};
use crate::service::payloader;
use mio::unix::SourceFd;
use mio::{Events, Interest, Poll, Token};
use std::os::unix::io::AsRawFd;

pub fn exec(
    processor_id: &String,
    processors: &Arc<RwLock<HashMap<String, Arc<Mutex<ProcessorBox>>>>>,
    input: ProcessorInput,
) -> Result<ProcessorResult, ProcessorError> {
    let processors_read = processors.read().unwrap();

    if let Some(c) = processors_read.get(processor_id.as_str()) {
        let mut processor = c.lock().unwrap();

        processor.run(input)
    } else {
        Err(format!("Processor with ID {} not found", processor_id))
    }
}

pub fn exec_agent(
    mut stream: UnixStream,
    processor_id: &String,
    processors: &Arc<RwLock<HashMap<String, Arc<Mutex<ProcessorBox>>>>>,
    input: ProcessorInput,
) {
    match exec(processor_id, processors, input) {
        Ok(processor_result) => match processor_result {
            ProcessorResult::None => return,
            ProcessorResult::Return(_) => return,
            ProcessorResult::PipeToProcessor(outputs) => {
                pipe_to_processor(stream, processors, outputs);
            }
            ProcessorResult::PipeToAgent(outputs) => {
                for (output, metadata) in outputs {
                    pipe_to_agent(&mut stream, processor_id, &output, metadata);
                }
            }
        },
        Err(e) => {
            eprintln!("Error executing processor {}: {}", processor_id, e);
            return;
        }
    }
}

pub fn pipe_to_agent(
    stream: &mut UnixStream,
    processor_id: &String,
    result: &ProcessorOutput,
    metadata: HashMap<String, String>,
) {
    let payload = match result {
        ProcessorOutput::Text(text) => payloader::build_from_str(&processor_id, text, metadata),
        ProcessorOutput::Binary(binary) => {
            payloader::build_from_binary(&processor_id, binary, metadata)
        }
        // TODO: Implement custom type serialization
        ProcessorOutput::Custom(_) => {
            panic!("Unsupported processor output type");
        }
    };

    match stream.write_all(&payload) {
        Ok(_) => {}
        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            let mut poll = Poll::new().unwrap();
            let mut events = Events::with_capacity(1);
            let token = Token(0);

            poll.registry()
                .register(
                    &mut SourceFd(&stream.as_raw_fd()),
                    token,
                    Interest::WRITABLE,
                )
                .unwrap();

            loop {
                poll.poll(&mut events, None).unwrap();
                for event in events.iter() {
                    if event.token() == token && event.is_writable() {
                        match stream.write_all(&payload) {
                            Ok(_) => {
                                // Deregister the stream from the poll instance.
                                poll.registry()
                                    .deregister(&mut SourceFd(&stream.as_raw_fd()))
                                    .unwrap();
                                return;
                            }
                            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                // The stream is still not ready, continue waiting.
                                continue;
                            }
                            Err(err) => {
                                eprintln!("Error writing to stream: {}", err);
                                // Deregister the stream from the poll instance.
                                poll.registry()
                                    .deregister(&mut SourceFd(&stream.as_raw_fd()))
                                    .unwrap();
                                return;
                            }
                        }
                    }
                }
            }
        }
        Err(err) => {
            eprintln!("Error writing to stream: {}", err);
        }
    }
}

fn pipe_to_processor(
    stream: UnixStream,
    processors: &Arc<RwLock<HashMap<String, Arc<Mutex<ProcessorBox>>>>>,
    outputs: Vec<(ProcessorId, ProcessorOutput, PayloadMetadata)>,
) {
    for (processor_id, output, metadata) in outputs {
        let mut stream_clone = stream.try_clone().unwrap();

        if let Some(_processor) = processors.read().unwrap().get(&processor_id) {
            let input = ProcessorInput::from(output);
            exec_agent(stream_clone, &processor_id, processors, input);
        } else {
            // Processor not found, try piping to the agent.
            pipe_to_agent(&mut stream_clone, &processor_id, &output, metadata);
        }
    }
}
