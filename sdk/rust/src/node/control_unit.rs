use std::io::Write;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::os::unix::net::UnixStream;

use serde::Serialize;

use crate::command;
use crate::pipeline::factory_registry::get_factory_ids;
use crate::service::payloader;
use crate::utility::path_helper;

use super::{NodeAction, OperatorNode};

#[derive(Serialize)]
struct ControlInitMessage {
    group_id: String,
    factory_ids: Vec<String>
}

pub fn open(
    group_id: String,
    tx: Arc<Mutex<mpsc::Sender<NodeAction>>>,
    operator: Arc<Mutex<OperatorNode>>
) -> thread::JoinHandle<()> {
    let socket_path = path_helper::control_socket_path();

    // Attempt to connect and unwrap immediately if successful
    let stream = UnixStream::connect(socket_path.clone())
        .expect(&format!("Failed to connect to control socket on path {}", socket_path));

    let stream_box = Arc::new(stream);

    handle_control_socket(group_id, tx, stream_box, operator)
}

fn handle_control_socket(
    group_id: String,
    tx: Arc<Mutex<mpsc::Sender<NodeAction>>>,
    stream_box: Arc<UnixStream>,
    operator: Arc<Mutex<OperatorNode>>
) -> thread::JoinHandle<()> {
    let stream_clone = Arc::clone(&stream_box);

    thread::spawn(move || {
        let control_init_message = ControlInitMessage {
            group_id: group_id.clone(),
            factory_ids: get_factory_ids()
        };

        let control_message = serde_json::to_string(&control_init_message).unwrap();

        let payload = payloader::build_from_str("connect_control_channel", &control_message);

        // Pass group id to the Aflecha.
        (&*stream_clone).write_all(&payload).unwrap();
        
        tx.lock().unwrap().send(NodeAction::EngineIdConfirmation).unwrap();

        loop {
            match payloader::extract_payload(&*stream_clone.clone()) {
                Ok((payload_id, message)) => {
                    let payload_message: String = String::from_utf8_lossy(&message).to_string();
                    let reply = command::handle_action(&operator, &payload_id, &payload_message);

                    (&*stream_clone).write_all(&reply).unwrap();
                }
                Err(_e) => {
                    // Wait 100ms to avoid busy waiting
                    // TODO: is it neccesary?
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
            } 
        }
    })
}

