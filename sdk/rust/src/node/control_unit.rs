use std::io::Write;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::os::unix::net::UnixStream;

use serde::Serialize;

use crate::pipeline::factory_registry::get_factory_ids;
use crate::service::payloader;
use crate::utility::path_helper;

use super::NodeAction;

#[derive(Serialize)]
struct ControlInitMessage {
    group_id: String,
    factory_ids: Vec<String>
}

fn handle_control_socket(
    group_id: String,
    tx: Arc<Mutex<mpsc::Sender<NodeAction>>>,
    stream_boxed: Arc<UnixStream>,  // Update the type here
) -> (thread::JoinHandle<()>, Arc<UnixStream>) {
    let stream_boxed_clone = Arc::clone(&stream_boxed);

    let thread = thread::spawn(move || {
        let control_init_message = ControlInitMessage {
            group_id: group_id.clone(),
            factory_ids: get_factory_ids()
        };

        let control_message = serde_json::to_string(&control_init_message).unwrap();

        let payload = payloader::build_from_str("connect_control_channel", &control_message);

        // Pass group id to the Aflecha.
        stream_boxed_clone.try_clone().unwrap().write_all(&payload).unwrap();
        
        tx.lock().unwrap().send(NodeAction::EngineIdConfirmation).unwrap();

        loop {
            match payloader::extract_payload(&*stream_boxed_clone) {
                Ok((payload_id, message)) => {
                    tx.lock().unwrap().send(NodeAction::ControlCommand(payload_id, message)).unwrap();
                }
                Err(_e) => {
                    // Wait 100ms to avoid busy waiting
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
            } 
        }
    });

    (thread, stream_boxed)
}

pub fn connect_control_socket(
    group_id: String,
    tx: Arc<Mutex<mpsc::Sender<NodeAction>>>,
) -> (thread::JoinHandle<()>, Arc<UnixStream>) {
    let socket_path = path_helper::control_socket_path();

    // Attempt to connect and unwrap immediately if successful
    let stream = UnixStream::connect(socket_path.clone())
        .expect(&format!("Failed to connect to control socket on path {}", socket_path));

    let stream_boxed = Arc::new(stream);

    handle_control_socket(group_id, tx, stream_boxed)
}
