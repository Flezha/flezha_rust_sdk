use serde::Deserialize;
use serde_json::Value;
use std::os::unix::net::UnixStream;
use std::io::Write;
use std::sync::{RwLock, Arc, Mutex};
use std::collections::HashMap;
use std::thread;
use std::str;

use crate::worker::{self, WorkerInput, Worker};
use crate::node::OperatorNode;
use crate::service::payloader;

use super::CommandAction;

use crate::utility::path_helper;

#[derive(Deserialize)]
pub struct ConnectSocketCommand {
    sockets_count: u8
}

impl CommandAction for ConnectSocketCommand {
    fn handle(
        &self,
        operator: &Arc<Mutex<OperatorNode>>,
        _message: &String,
        reference_id: String,
    ) {
        let operator_guard = operator.lock().unwrap();
        let workers = operator_guard.get_workers();
        let control_stream = operator_guard.get_control_stream();
        let io_streams = operator_guard.get_io_streams();
        let reference_id = reference_id.clone();
        let operator_id = operator_guard.get_operator_id();

        drop(operator_guard);

        for key in 0..self.sockets_count {
            let workers_clone = Arc::clone(&workers);
            let stream = connect_unix(&operator_id, "primary".to_string());
            let shared_stream = connect_unix(&operator_id, "secondary".to_string());

            io_streams.write().unwrap().insert(key, shared_stream.try_clone().unwrap());

            let _handle = thread::spawn(move || {
                listen_incoming(stream, &workers_clone)
            });
        }

        let mut hashmap: HashMap<String, Value> = HashMap::new();
        hashmap.insert("reference_id".to_string(), Value::String(reference_id));

        let json_string = serde_json::to_string(&hashmap).unwrap();
        let payload = payloader::build_from_str(&"connect_sockets".to_string(), &json_string);
        control_stream.try_clone().unwrap().write_all(&payload).unwrap();
    }
}

fn connect_unix(operator_id: &String, stream_type: String) -> UnixStream {
    let socket_path = path_helper::stream_socket_path(operator_id, stream_type);
    
    UnixStream::connect(socket_path.clone())
        .expect(&format!("Failed to connect to the socket: {}", socket_path.clone()))
}

fn listen_incoming(mut stream: UnixStream, workers: &Arc<RwLock<HashMap<String, Arc<Mutex<Box<dyn Worker>>>>>>) {
    loop {
        match payloader::extract_payload(&mut stream) {
            Ok((worker_id, mut payload)) => {
                // input_type is the first byte of the payload
                let input_type = payload[0];

                // processing_type is the second byte of the payload
                let processing_type = payload[1];

                payload.remove(0);
                payload.remove(0);

                let input = match input_type {
                    1 => WorkerInput::Binary(payload),
                    2 => WorkerInput::Text(str::from_utf8(&payload).unwrap().to_string()),
                    types => panic!("Invalid input type {}", types),
                };

                match processing_type {
                    1 => {
                        worker::exec(
                            &worker_id,
                            workers,
                            &mut stream, 
                            input
                        )
                    },
                    2 => {
                        let workers = Arc::clone(workers);
                        let mut stream = stream.try_clone().unwrap();

                        thread::spawn(move || {
                            worker::exec(
                                &worker_id,
                                &workers,
                                &mut stream, 
                                input
                            )
                        });

                        None
                    }
                    _ => panic!("Invalid processing type"),
                };
            }
            Err(_e) => {
                // TODO: log error
                break;
            }
        }
    }
}