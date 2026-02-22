use std::io::Write;
use std::sync::{Arc, Mutex, RwLock};

use std::collections::HashMap;

use crate::processor::factory_registry::ProcessorBox;
use crate::processor::{
    PayloadMetadata, ProcessorError, ProcessorId, ProcessorInput, ProcessorOutput, ProcessorResult,
};
use crate::service::payloader;
use crate::transport::FlezhaStream;
use mio::unix::SourceFd;
use mio::{Events, Interest, Poll, Token};

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
    stream: Arc<FlezhaStream>,
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
                    pipe_to_agent(&stream, processor_id, &output, metadata);
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
    stream: &FlezhaStream,
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

    let mut writer: &FlezhaStream = stream;
    match writer.write_all(&payload) {
        Ok(_) => {}
        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            if let Some(raw_fd) = stream.as_raw_fd() {
                let mut poll = Poll::new().unwrap();
                let mut events = Events::with_capacity(1);
                let token = Token(0);

                poll.registry()
                    .register(
                        &mut SourceFd(&raw_fd),
                        token,
                        Interest::WRITABLE,
                    )
                    .unwrap();

                loop {
                    poll.poll(&mut events, None).unwrap();
                    for event in events.iter() {
                        if event.token() == token && event.is_writable() {
                            match writer.write_all(&payload) {
                                Ok(_) => {
                                    poll.registry()
                                        .deregister(&mut SourceFd(&raw_fd))
                                        .unwrap();
                                    return;
                                }
                                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                    continue;
                                }
                                Err(err) => {
                                    eprintln!("Error writing to stream: {}", err);
                                    poll.registry()
                                        .deregister(&mut SourceFd(&raw_fd))
                                        .unwrap();
                                    return;
                                }
                            }
                        }
                    }
                }
            }
            // For non-Unix streams (e.g., QUIC), WouldBlock is handled internally
        }
        Err(err) => {
            eprintln!("Error writing to stream: {}", err);
        }
    }
}

fn pipe_to_processor(
    stream: Arc<FlezhaStream>,
    processors: &Arc<RwLock<HashMap<String, Arc<Mutex<ProcessorBox>>>>>,
    outputs: Vec<(ProcessorId, ProcessorOutput, PayloadMetadata)>,
) {
    for (processor_id, output, metadata) in outputs {
        if let Some(_processor) = processors.read().unwrap().get(&processor_id) {
            let input = ProcessorInput::from(output);
            exec_agent(Arc::clone(&stream), &processor_id, processors, input);
        } else {
            // Processor not found, try piping to the agent.
            pipe_to_agent(&stream, &processor_id, &output, metadata);
        }
    }
}
