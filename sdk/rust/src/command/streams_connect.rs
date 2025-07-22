use super::CommandAction;
use crate::pb::ConnectSocket;

use crossbeam::channel::Sender;
use std::os::unix::net::UnixStream;
use std::str;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::node::coordinator::{CoordinatorMessage, WorkerType};
use crate::node::OperatorNode;
use crate::pipeline::PipelineInput;
use crate::service::payloader;

use crate::utility::path_helper;

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

        // TODO: Implement error handling.

        drop(operator_guard);

        for key in 0..self.sockets_count {
            let ctx_clone = ctx.lock().unwrap().clone();
            let stream = connect_unix(&operator_id, "primary".to_string());
            let shared_stream = connect_unix(&operator_id, "secondary".to_string());

            io_streams
                .write()
                .unwrap()
                .insert(key, shared_stream.try_clone().unwrap());

            let _handle = thread::spawn(move || listen_incoming(stream, ctx_clone));
        }

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

fn listen_incoming(mut stream: UnixStream, ctx: Sender<CoordinatorMessage>) {
    loop {
        match payloader::extract_payload(&mut stream) {
            Ok((pipeline_id, mut payload)) => {
                // input_type is the first byte of the payload
                let input_type = payload[0];

                // worker_type is the second byte of the payload
                let worker_type_int = payload[1];

                let worker_type = match worker_type_int {
                    1 => WorkerType::ThreadPool,
                    2 => WorkerType::ThreadSpawn,
                    _ => panic!("Invalid worker type"),
                };

                payload.remove(0);
                payload.remove(0);

                let input = match input_type {
                    1 => PipelineInput::Binary(payload),
                    2 => PipelineInput::Text(str::from_utf8(&payload).unwrap().to_string()),
                    types => panic!("Invalid input type {}", types),
                };

                ctx.send(CoordinatorMessage::WorkerCheckOut(
                    pipeline_id,
                    input,
                    worker_type,
                    stream.try_clone().unwrap(),
                ))
                .expect("Failed to send message to coordinator");
            }
            Err(_e) => {
                // TODO: log error
                break;
            }
        }
    }
}
