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
use crate::transport::config::{get_transport_type, TransportType};
use crate::transport::quic::QuicClient;
use crate::transport::FlezhaStream;
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
) -> (thread::JoinHandle<()>, Arc<FlezhaStream>, Option<Arc<QuicClient>>) {
    match get_transport_type() {
        TransportType::Uds => {
            let (handle, stream) = open_uds(namespace, itx, otx);
            (handle, stream, None)
        }
        TransportType::Quic => open_quic(namespace, itx, otx),
    }
}

fn open_uds(
    namespace: String,
    itx: Arc<Mutex<Sender<ClientResponseMessage>>>,
    otx: Arc<Mutex<Sender<ComputeEngineMessage>>>,
) -> (thread::JoinHandle<()>, Arc<FlezhaStream>) {
    let socket_path = path_helper::control_socket_path();

    let stream = UnixStream::connect(socket_path.clone()).expect(&format!(
        "Failed to connect to control socket on path {}",
        socket_path
    ));

    let stream_box = Arc::new(FlezhaStream::Unix(stream));

    sock_conn(namespace, stream_box, itx, otx)
}

fn open_quic(
    namespace: String,
    itx: Arc<Mutex<Sender<ClientResponseMessage>>>,
    otx: Arc<Mutex<Sender<ComputeEngineMessage>>>,
) -> (thread::JoinHandle<()>, Arc<FlezhaStream>, Option<Arc<QuicClient>>) {
    let address = crate::transport::config::quic_address();
    let ca_cert_path = crate::transport::config::ca_cert_path();

    let client = QuicClient::connect(&address, &ca_cert_path)
        .expect("Failed to connect to QUIC server");

    let client = Arc::new(client);

    // Open first bidi stream as control channel
    let control_stream = client
        .open_bidi_stream()
        .expect("Failed to open QUIC control stream");

    let stream_box = Arc::new(FlezhaStream::Quic(control_stream));

    let (handle, stream) = sock_conn(namespace, stream_box, itx, otx);

    (handle, stream, Some(client))
}

fn sock_conn(
    namespace: String,
    stream_box: Arc<FlezhaStream>,
    itx: Arc<Mutex<Sender<ClientResponseMessage>>>,
    otx: Arc<Mutex<Sender<ComputeEngineMessage>>>,
) -> (thread::JoinHandle<()>, Arc<FlezhaStream>) {
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
    stream_clone: Arc<FlezhaStream>,
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
