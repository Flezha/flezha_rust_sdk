use serde::Deserialize;
use std::os::unix::net::UnixStream;
use std::sync::{RwLock, Arc, Mutex};
use std::collections::HashMap;
use std::thread;
use std::str;

use crate::pipeline::factory_registry::PipelineBox;
use crate::pipeline::PipelineInput;
use crate::pipeline::runner;
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
        _errors: &mut Vec<String>
    ) -> (String, String) {   
        let operator_guard = operator.lock().unwrap();
        let pipelines = operator_guard.get_pipelines();
        let io_streams = operator_guard.get_io_streams();
        let operator_id = operator_guard.get_operator_id();

        // TODO: Implement error handling.

        drop(operator_guard);

        for key in 0..self.sockets_count {
            let pipelines_clone = Arc::clone(&pipelines);
            let stream = connect_unix(&operator_id, "primary".to_string());
            let shared_stream = connect_unix(&operator_id, "secondary".to_string());

            io_streams.write().unwrap().insert(key, shared_stream.try_clone().unwrap());

            let _handle = thread::spawn(move || {
                listen_incoming(stream, &pipelines_clone)
            });
        }

        ("connect_sockets".to_string(), "Connection to socket has been established".to_string())
    }
}

fn connect_unix(operator_id: &String, stream_type: String) -> UnixStream {
    let socket_path = path_helper::stream_socket_path(operator_id, stream_type);
    
    UnixStream::connect(socket_path.clone())
        .expect(&format!("Failed to connect to the socket: {}", socket_path.clone()))
}

fn listen_incoming(mut stream: UnixStream, pipelines: &Arc<RwLock<HashMap<String, Arc<Mutex<PipelineBox>>>>>) {
    loop {
        match payloader::extract_payload(&mut stream) {
            Ok((pipeline_id, mut payload)) => {
                // input_type is the first byte of the payload
                let input_type = payload[0];

                // processing_type is the second byte of the payload
                let processing_type = payload[1];

                payload.remove(0);
                payload.remove(0);

                let input = match input_type {
                    1 => PipelineInput::Binary(payload),
                    2 => PipelineInput::Text(str::from_utf8(&payload).unwrap().to_string()),
                    types => panic!("Invalid input type {}", types),
                };

                match processing_type {
                    1 => runner::exec_agent(&mut stream, &pipeline_id, pipelines, input),
                    2 => {
                        let pipelines = Arc::clone(pipelines);
                        let mut stream = stream.try_clone().unwrap();
    
                        thread::spawn(move || {
                            runner::exec_agent(&mut stream, &pipeline_id, &pipelines, input)
                        });
                    },
                    _ => panic!("Invalid processing type"),
                }
            }
            Err(_e) => {
                // TODO: log error
                break;
            }
        }
    }
}