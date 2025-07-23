use crossbeam::channel::Sender;
use std::io::Write;
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde::Serialize;

use crate::command::{self, CommandActionType};
use crate::computation::factory_registry::get_factory_ids;
use crate::service::payloader;
use crate::utility::path_helper;

use super::{NodeAction, OperatorNode};

#[derive(Serialize)]
struct ControlInitMessage {
    group_id: String,
    factory_ids: Vec<String>,
}

pub fn open(
    group_id: String,
    tx: Arc<Mutex<Sender<NodeAction>>>,
    operator: Arc<Mutex<OperatorNode>>,
) -> thread::JoinHandle<()> {
    let socket_path = path_helper::control_socket_path();

    // Attempt to connect and unwrap immediately if successful
    let stream = UnixStream::connect(socket_path.clone()).expect(&format!(
        "Failed to connect to control socket on path {}",
        socket_path
    ));

    let stream_box = Arc::new(stream);

    sock_conn(group_id, tx, stream_box, operator)
}

fn sock_conn(
    group_id: String,
    tx: Arc<Mutex<Sender<NodeAction>>>,
    stream_box: Arc<UnixStream>,
    operator: Arc<Mutex<OperatorNode>>,
) -> thread::JoinHandle<()> {
    let stream_clone = Arc::clone(&stream_box);

    thread::spawn(move || {
        send_init_msg(group_id, tx, stream_clone.clone());

        loop {
            match payloader::extract_payload(&*stream_clone.clone()) {
                Ok((payload_id, message, _virtual_channel)) => {
                    let payload_message: String = String::from_utf8_lossy(&message).to_string();

                    let (handler_type, reference_id) =
                        command::get_handler(&payload_id, &payload_message);

                    match handler_type {
                        CommandActionType::Regular(handler) => {
                            let reply = command::handle_action(
                                &operator,
                                handler,
                                &payload_id,
                                &payload_message,
                                reference_id,
                            );
                            (&*stream_clone).write_all(&reply).unwrap();
                        }

                        CommandActionType::OperatorDisconnect(handler) => {
                            let reply = command::handle_action(
                                &operator,
                                handler,
                                &payload_id,
                                &payload_message,
                                reference_id,
                            );
                            (&*stream_clone).write_all(&reply).unwrap();

                            // Close the socket.
                            stream_clone.shutdown(Shutdown::Both).unwrap();

                            break;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error extracting payload: {}", e);
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
            }
        }
    })
}

fn send_init_msg(
    group_id: String,
    tx: Arc<Mutex<Sender<NodeAction>>>,
    stream_clone: Arc<UnixStream>,
) {
    let control_init_message = ControlInitMessage {
        group_id: group_id.clone(),
        factory_ids: get_factory_ids(),
    };

    let control_message = serde_json::to_string(&control_init_message).unwrap();
    let payload = payloader::build_from_str("connect_control_channel", 1, &control_message);

    // Pass group id to the Flezha.
    (&*stream_clone).write_all(&payload).unwrap();

    tx.lock()
        .unwrap()
        .send(NodeAction::EngineIdConfirmation)
        .unwrap();
}
