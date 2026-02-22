use super::CommandAction;
use crate::pb::ConnectSocket;

use crossbeam::channel::Sender;
use mio::unix::SourceFd;
use mio::{Events, Interest, Poll, Token};
use std::collections::HashMap;
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixStream;
use std::str;
use std::sync::Arc;
use std::thread;

use crate::compute_engine::{ComputeEngineMessage, ComputeEngineNode};
use crate::processor::ProcessorInput;
use crate::service::payloader;
use crate::transport::config::{get_transport_type, TransportType};
use crate::transport::protocol;
use crate::transport::FlezhaStream;

use crate::utility::path_helper;

const EVENTS_CAPACITY: usize = 1024;

impl CommandAction for ConnectSocket {
    fn handle(
        &self,
        compute_engine: &mut ComputeEngineNode,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        match get_transport_type() {
            TransportType::Uds => handle_uds(self, compute_engine),
            TransportType::Quic => handle_quic(self, compute_engine),
        }
    }
}

fn handle_uds(
    cmd: &ConnectSocket,
    compute_engine: &mut ComputeEngineNode,
) -> (String, Vec<u8>) {
    let io_streams = compute_engine.get_io_streams();
    let compute_engine_id = compute_engine.get_compute_engine_id();
    let ctx = compute_engine.get_compute_engine_channel();

    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(EVENTS_CAPACITY);
    let mut streams = HashMap::new();

    for key in 0..cmd.sockets_count {
        let stream = connect_unix(&compute_engine_id, "primary".to_string());
        stream.set_nonblocking(true).unwrap();
        let shared_stream = connect_unix(&compute_engine_id, "secondary".to_string());

        io_streams
            .write()
            .unwrap()
            .insert(key, FlezhaStream::Unix(shared_stream.try_clone().unwrap()));

        let token = Token(key as usize);
        poll.registry()
            .register(
                &mut SourceFd(&stream.as_raw_fd()),
                token,
                Interest::READABLE,
            )
            .unwrap();

        streams.insert(token, Arc::new(FlezhaStream::Unix(stream)));
    }

    let _handle = thread::spawn(move || {
        listen_incoming_uds(poll, &mut events, &mut streams, ctx.lock().unwrap().clone())
    });

    ("connect_sockets".to_string(), Vec::new())
}

fn handle_quic(
    cmd: &ConnectSocket,
    compute_engine: &mut ComputeEngineNode,
) -> (String, Vec<u8>) {
    let io_streams = compute_engine.get_io_streams();
    let ctx = compute_engine.get_compute_engine_channel();

    let quic_client = compute_engine
        .get_quic_client()
        .expect("QUIC client not available");

    let mut primary_streams = Vec::new();

    for key in 0..cmd.sockets_count {
        // Open primary stream and send type identifier
        let mut primary = quic_client
            .open_bidi_stream()
            .expect("Failed to open primary QUIC stream");
        primary
            .write_all(&[protocol::STREAM_TYPE_PRIMARY])
            .expect("Failed to send primary stream type");

        // Open secondary stream and send type identifier
        let mut secondary = quic_client
            .open_bidi_stream()
            .expect("Failed to open secondary QUIC stream");
        secondary
            .write_all(&[protocol::STREAM_TYPE_SECONDARY])
            .expect("Failed to send secondary stream type");

        io_streams
            .write()
            .unwrap()
            .insert(key, FlezhaStream::Quic(secondary.try_clone()));

        primary_streams.push(FlezhaStream::Quic(primary));
    }

    // For QUIC, we use blocking reads in a thread instead of mio polling
    let ctx_clone = ctx.lock().unwrap().clone();
    let _handle = thread::spawn(move || {
        listen_incoming_quic(primary_streams, ctx_clone);
    });

    ("connect_sockets".to_string(), Vec::new())
}

fn connect_unix(compute_engine_id: &String, stream_type: String) -> UnixStream {
    let socket_path = path_helper::stream_socket_path(compute_engine_id, stream_type);

    UnixStream::connect(socket_path.clone()).expect(&format!(
        "Failed to connect to the socket: {}",
        socket_path.clone()
    ))
}

fn listen_incoming_uds(
    mut poll: Poll,
    events: &mut Events,
    streams: &mut HashMap<Token, Arc<FlezhaStream>>,
    ctx: Sender<ComputeEngineMessage>,
) {
    loop {
        poll.poll(events, None).unwrap();

        for event in events.iter() {
            let token = event.token();
            if let Some(stream) = streams.get_mut(&token) {
                loop {
                    match payloader::extract_payload_non_blocking(&mut &**stream) {
                        Ok(Some((processor_id, payload, metadata))) => {
                            let input = parse_input(payload, &metadata);

                            ctx.send(ComputeEngineMessage::WorkerCheckOut(
                                processor_id,
                                input,
                                Arc::clone(stream),
                            ))
                            .expect("Failed to send message to coordinator");
                        }
                        Ok(None) => {
                            // Would block
                            break;
                        }
                        Err(_e) => {
                            streams.remove(&token);
                            break;
                        }
                    }
                }
            }
        }
    }
}

fn listen_incoming_quic(
    mut streams: Vec<FlezhaStream>,
    ctx: Sender<ComputeEngineMessage>,
) {
    // Spawn a thread per QUIC primary stream for blocking reads
    let mut handles = Vec::new();

    for stream in streams.drain(..) {
        let ctx = ctx.clone();
        let stream = Arc::new(stream);
        let handle = thread::spawn(move || {
            loop {
                match payloader::extract_payload(&mut &*stream) {
                    Ok((processor_id, payload, metadata)) => {
                        let input = parse_input(payload, &metadata);

                        ctx.send(ComputeEngineMessage::WorkerCheckOut(
                            processor_id,
                            input,
                            Arc::clone(&stream),
                        ))
                        .expect("Failed to send message to coordinator");
                    }
                    Err(_e) => {
                        break;
                    }
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all stream readers to finish
    for handle in handles {
        let _ = handle.join();
    }
}

fn parse_input(payload: Vec<u8>, metadata: &HashMap<String, String>) -> ProcessorInput {
    let default_type = "1".to_string();
    let input_type = metadata.get("input_type").unwrap_or(&default_type);

    match input_type.as_str() {
        "1" => ProcessorInput::Binary(payload),
        "2" => ProcessorInput::Text(str::from_utf8(&payload).unwrap().to_string()),
        err_type => panic!("Invalid input type {}", err_type),
    }
}
