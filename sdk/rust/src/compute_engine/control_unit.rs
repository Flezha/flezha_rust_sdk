use crossbeam::channel::Sender;
use std::collections::HashMap;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde::Serialize;

use crate::compute_engine::ComputeEngineMessage;
use crate::processor::factory_registry::get_factory_ids;
use crate::service::payloader;
use crate::utility::path_helper;

use super::ClientResponseMessage;

#[derive(Serialize)]
struct ControlInitMessage {
    namespace: String,
    factory_ids: Vec<String>,
}

pub fn open(
    namespace: String,
    itx: Arc<Mutex<Sender<ClientResponseMessage>>>,
    otx: Arc<Mutex<Sender<ComputeEngineMessage>>>,
) -> (thread::JoinHandle<()>, Arc<UnixStream>) {
    let socket_path = path_helper::control_socket_path();

    // Attempt to connect and unwrap immediately if successful
    let stream = UnixStream::connect(socket_path.clone()).expect(&format!(
        "Failed to connect to control socket on path {}",
        socket_path
    ));

    let stream_box = Arc::new(stream);

    sock_conn(namespace, stream_box, itx, otx)
}

fn sock_conn(
    namespace: String,
    stream_box: Arc<UnixStream>,
    itx: Arc<Mutex<Sender<ClientResponseMessage>>>,
    otx: Arc<Mutex<Sender<ComputeEngineMessage>>>,
) -> (thread::JoinHandle<()>, Arc<UnixStream>) {
    let stream_clone = Arc::clone(&stream_box);

    let handle = thread::spawn(move || {
        send_init_msg(namespace, itx, stream_clone.clone());

        loop {
            match payloader::extract_payload(&*stream_clone.clone()) {
                Ok((payload_id, message, metadata)) => {
                    otx.lock()
                        .unwrap()
                        .send(ComputeEngineMessage::HandleCommandAction(
                            payload_id, message, metadata,
                        ))
                        .unwrap();
                }
                Err(_e) => {
                    // Returns errors when testing.
                    // eprint!("Error extracting payload: {}", e);
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
            }
        }
    });

    (handle, stream_box)
}

fn send_init_msg(
    namespace: String,
    tx: Arc<Mutex<Sender<ClientResponseMessage>>>,
    stream_clone: Arc<UnixStream>,
) {
    let control_init_message = ControlInitMessage {
        namespace: namespace.clone(),
        factory_ids: get_factory_ids(),
    };

    let control_message = serde_json::to_string(&control_init_message).unwrap();
    let payload =
        payloader::build_from_str("connect_control_channel", &control_message, HashMap::new());

    // Pass namespace id to the Flezha.
    (&*stream_clone).write_all(&payload).unwrap();

    tx.lock()
        .unwrap()
        .send(ClientResponseMessage::ComputeEngineIdConfirmation)
        .unwrap();
}
