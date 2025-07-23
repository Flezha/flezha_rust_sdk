use std::io::Write;
use std::sync::{Arc, Mutex, RwLock};
use std::{collections::HashMap, os::unix::net::UnixStream};

use crate::computation::factory_registry::ComputationBox;
use crate::computation::{ComputationError, ComputationInput, ComputationOutput, ComputationResult, VirtualChannel};
use crate::service::payloader;
use mio::{Events, Interest, Poll, Token};
use std::os::unix::io::AsRawFd;
use mio::unix::SourceFd;

pub fn exec(
    computation_id: &String,
    computations: &Arc<RwLock<HashMap<String, Arc<Mutex<ComputationBox>>>>>,
    input: ComputationInput,
) -> Result<ComputationResult, ComputationError> {
    let computations_read = computations.read().unwrap();

    if let Some(c) = computations_read.get(computation_id.as_str()) {
        let mut computation = c.lock().unwrap();

        computation.run(input)
    } else {
        panic!("Computation with given ID not found {}", computation_id);
    }
}

pub fn exec_agent(
    mut stream: UnixStream,
    computation_id: &String,
    computations: &Arc<RwLock<HashMap<String, Arc<Mutex<ComputationBox>>>>>,
    input: ComputationInput,
) {
    match exec(computation_id, computations, input) {
        Ok(computation_result) => {
            match computation_result {
                ComputationResult::None => {
                    return;
                }
                ComputationResult::Forward(outputs) => {
                    for (output, virtual_channel) in outputs {
                        pipe_to_agent(&mut stream, computation_id, &output, virtual_channel);
                    }
                }
            }
        },
        Err(e) => {
            eprintln!("Error executing computation {}: {}", computation_id, e);
            return;
        }
    }
}

pub fn pipe_to_agent(
    stream: &mut UnixStream,
    computation_id: &String,
    result: &ComputationOutput,
    virtual_channel: VirtualChannel,
) {
    // TODO: Hardcoded virtual channel.
    let payload = match result {
        ComputationOutput::Text(text) => {
            payloader::build_from_str(&computation_id, virtual_channel, text)
        }
        ComputationOutput::Binary(binary) => {
            payloader::build_from_binary(&computation_id, virtual_channel, binary)
        }
        // TODO: Implement custom type serialization
        ComputationOutput::Custom(_) => {
            panic!("Unsupported computation output type");
        }
    };

    match stream.write_all(&payload) {
        Ok(_) => {}
        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            let mut poll = Poll::new().unwrap();
            let mut events = Events::with_capacity(1);
            let token = Token(0);

            poll.registry()
                .register(&mut SourceFd(&stream.as_raw_fd()), token, Interest::WRITABLE)
                .unwrap();

            loop {
                poll.poll(&mut events, None).unwrap();
                for event in events.iter() {
                    if event.token() == token && event.is_writable() {
                        match stream.write_all(&payload) {
                            Ok(_) => {
                                // Deregister the stream from the poll instance.
                                poll.registry().deregister(&mut SourceFd(&stream.as_raw_fd())).unwrap();
                                return;
                            },
                            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                // The stream is still not ready, continue waiting.
                                continue;
                            }
                            Err(err) => {
                                eprintln!("Error writing to stream: {}", err);
                                // Deregister the stream from the poll instance.
                                poll.registry().deregister(&mut SourceFd(&stream.as_raw_fd())).unwrap();
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
