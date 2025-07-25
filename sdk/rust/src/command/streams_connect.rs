use super::CommandAction;
use crate::pb::ConnectSocket;

use crossbeam::channel::Sender;
use mio::unix::SourceFd;
use mio::{Events, Interest, Poll, Token};
use std::collections::HashMap;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixStream;
use std::str;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::computation::ComputationInput;
use crate::node::coordinator::{CoordinatorMessage, WorkerType};
use crate::node::OperatorNode;
use crate::service::payloader;

use crate::utility::path_helper;

const EVENTS_CAPACITY: usize = 1024;

impl CommandAction for ConnectSocket {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let operator_guard = operator.lock().unwrap();
        let io_streams = operator_guard.get_io_streams();
        let operator_id = operator_guard.get_operator_id();
        let ctx = operator_guard.get_coordinator_channel();

        drop(operator_guard);

        let poll = Poll::new().unwrap();
        let mut events = Events::with_capacity(EVENTS_CAPACITY);
        let mut streams = HashMap::new();

        for key in 0..self.sockets_count {
            let stream = connect_unix(&operator_id, "primary".to_string());
            stream.set_nonblocking(true).unwrap();
            let shared_stream = connect_unix(&operator_id, "secondary".to_string());

            io_streams
                .write()
                .unwrap()
                .insert(key, shared_stream.try_clone().unwrap());

            let token = Token(key as usize);
            poll.registry()
                .register(
                    &mut SourceFd(&stream.as_raw_fd()),
                    token,
                    Interest::READABLE,
                )
                .unwrap();

            streams.insert(token, stream);
        }

        let _handle = thread::spawn(move || {
            listen_incoming(poll, &mut events, &mut streams, ctx.lock().unwrap().clone())
        });

        ("connect_sockets".to_string(), Vec::new())
    }
}

fn connect_unix(operator_id: &String, stream_type: String) -> UnixStream {
    let socket_path = path_helper::stream_socket_path(operator_id, stream_type);

    UnixStream::connect(socket_path.clone()).expect(&format!(
        "Failed to connect to the socket: {}",
        socket_path.clone()
    ))
}

fn listen_incoming(
    mut poll: Poll,
    events: &mut Events,
    streams: &mut HashMap<Token, UnixStream>,
    ctx: Sender<CoordinatorMessage>,
) {
    loop {
        poll.poll(events, None).unwrap();

        for event in events.iter() {
            let token = event.token();
            if let Some(stream) = streams.get_mut(&token) {
                loop {
                    match payloader::extract_payload_non_blocking(&mut *stream) {
                        Ok(Some((computation_id, payload, metadata))) => {
                            let worker_type = match metadata.get("worker_type".into()).unwrap_or(&"1".into()).as_str() {
                                "1" => WorkerType::ThreadPool,
                                "2" => WorkerType::ThreadSpawn,
                                _ => panic!("Invalid worker type"),
                            };

                            let input = match metadata.get("input_type".into()).unwrap_or(&"1".into()).as_str() {
                                "1" => ComputationInput::Binary(payload),
                                "2" => ComputationInput::Text(
                                    str::from_utf8(&payload).unwrap().to_string(),
                                ),
                                err_type => panic!("Invalid input type {}", err_type),
                            };

                            ctx.send(CoordinatorMessage::WorkerCheckOut(
                                computation_id,
                                input,
                                worker_type,
                                stream.try_clone().unwrap(),
                            ))
                            .expect("Failed to send message to coordinator");
                        }
                        Ok(None) => {
                            // Would block
                            break;
                        }
                        Err(_e) => {
                            // Returns errors when testing.
                            // eprint!("Error extracting payload: {}", e);
                            streams.remove(&token);
                            break;
                        }
                    }
                }
            }
        }
    }
}
