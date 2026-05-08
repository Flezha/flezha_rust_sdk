use crossbeam::channel::Sender;
use std::collections::HashMap;
use std::io::{self, Write};
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde::Serialize;

use crate::executor::ExecutorMessage;
use crate::relay::registry::class_factory_ids;
use flz_transport::service::payloader;
use flz_transport::transport::config::{get_transport_type, TransportType};
use flz_transport::transport::quic::QuicClient;
use flz_transport::transport::FlezhaStream;
use flz_transport::utility::path_helper;

use super::ClientResponseMessage;

#[derive(Serialize)]
struct FactoryInfo {
    id: String,
    description: Option<String>,
}

#[derive(Serialize)]
struct ControlInitMessage {
    namespace: String,
    factory_ids: Vec<String>,
    factories: Vec<FactoryInfo>,
}

/// Initial connection with retry and startup confirmation.
pub fn open(
    namespace: String,
    itx: Arc<Mutex<Sender<ClientResponseMessage>>>,
    otx: Arc<Mutex<Sender<ExecutorMessage>>>,
) -> (thread::JoinHandle<()>, Arc<FlezhaStream>, Option<Arc<QuicClient>>) {
    let (handle, stream, quic_client) =
        open_with_retry(namespace, Some(itx), otx);
    (handle, stream, quic_client)
}

/// Reconnection entry point — no startup confirmation sent.
pub fn open_reconnect(
    namespace: String,
    otx: Arc<Mutex<Sender<ExecutorMessage>>>,
) -> (thread::JoinHandle<()>, Arc<FlezhaStream>, Option<Arc<QuicClient>>) {
    open_with_retry(namespace, None, otx)
}

fn open_with_retry(
    namespace: String,
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
    otx: Arc<Mutex<Sender<ExecutorMessage>>>,
) -> (thread::JoinHandle<()>, Arc<FlezhaStream>, Option<Arc<QuicClient>>) {
    let mut attempt = 0u32;
    let max_backoff = Duration::from_secs(30);

    loop {
        match try_open(namespace.clone(), itx.clone(), otx.clone()) {
            Ok(result) => {
                if attempt > 0 {
                    eprintln!("[flezha] Connected after {} attempts", attempt);
                }
                return result;
            }
            Err(e) => {
                attempt += 1;
                let backoff = std::cmp::min(
                    Duration::from_millis(100 * 2u64.pow(attempt.min(8))),
                    max_backoff,
                );
                eprintln!(
                    "[flezha] Connection attempt {} failed: {}. Retrying in {:?}...",
                    attempt, e, backoff
                );
                thread::sleep(backoff);
            }
        }
    }
}

fn try_open(
    namespace: String,
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
    otx: Arc<Mutex<Sender<ExecutorMessage>>>,
) -> Result<(thread::JoinHandle<()>, Arc<FlezhaStream>, Option<Arc<QuicClient>>), String> {
    match get_transport_type() {
        TransportType::Uds => {
            let (handle, stream) = try_open_uds(namespace, itx, otx)?;
            Ok((handle, stream, None))
        }
        TransportType::Quic => try_open_quic(namespace, itx, otx),
    }
}

fn try_open_uds(
    namespace: String,
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
    otx: Arc<Mutex<Sender<ExecutorMessage>>>,
) -> Result<(thread::JoinHandle<()>, Arc<FlezhaStream>), String> {
    let socket_path = path_helper::control_socket_path();

    let stream = UnixStream::connect(socket_path.clone())
        .map_err(|e| format!("Failed to connect to control socket {}: {}", socket_path, e))?;

    let stream_box = Arc::new(FlezhaStream::Unix(stream));

    Ok(sock_conn(namespace, stream_box, itx, otx))
}

fn try_open_quic(
    namespace: String,
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
    otx: Arc<Mutex<Sender<ExecutorMessage>>>,
) -> Result<(thread::JoinHandle<()>, Arc<FlezhaStream>, Option<Arc<QuicClient>>), String> {
    let address = flz_transport::transport::config::quic_address();
    let ca_cert_path = flz_transport::transport::config::ca_cert_path();

    let client = QuicClient::connect(&address, &ca_cert_path)
        .map_err(|e| format!("Failed to connect to QUIC server: {}", e))?;

    let client = Arc::new(client);

    let control_stream = client
        .open_bidi_stream()
        .map_err(|e| format!("Failed to open QUIC control stream: {}", e))?;

    let stream_box = Arc::new(FlezhaStream::Quic(control_stream));

    let (handle, stream) = sock_conn(namespace, stream_box, itx, otx);

    Ok((handle, stream, Some(client)))
}

fn sock_conn(
    namespace: String,
    stream_box: Arc<FlezhaStream>,
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
    otx: Arc<Mutex<Sender<ExecutorMessage>>>,
) -> (thread::JoinHandle<()>, Arc<FlezhaStream>) {
    let stream_clone = Arc::clone(&stream_box);

    let handle = thread::spawn(move || {
        send_init_msg(namespace, stream_clone.clone());

        // Send startup confirmation only on initial connection
        if let Some(itx) = itx {
            itx.lock()
                .unwrap()
                .send(ClientResponseMessage::ExecutorIdConfirmation)
                .unwrap();
        }

        loop {
            match payloader::extract_payload(&*stream_clone.clone()) {
                Ok((payload_id, message, metadata)) => {
                    otx.lock()
                        .unwrap()
                        .send(ExecutorMessage::HandleCommandAction(
                            payload_id, message, metadata,
                        ))
                        .unwrap();
                }
                Err(e) => match e.kind() {
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted => {
                        thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                    _ => {
                        // Disconnects are expected during graceful controller
                        // shutdown / test teardown. Surfacing them via
                        // `FLEZHA_LOG=debug` keeps the controller's stdout
                        // clean while still allowing executors to opt in.
                        if std::env::var("FLEZHA_LOG").as_deref() == Ok("debug") {
                            eprintln!("[flezha] Control channel disconnected: {}", e);
                        }
                        let _ = otx
                            .lock()
                            .unwrap()
                            .send(ExecutorMessage::Reconnect);
                        break;
                    }
                },
            }
        }
    });

    (handle, stream_box)
}

fn send_init_msg(namespace: String, stream_clone: Arc<FlezhaStream>) {
    let factory_ids: Vec<String> = class_factory_ids().iter().map(|s| s.to_string()).collect();
    let factories: Vec<FactoryInfo> = factory_ids
        .iter()
        .map(|id| FactoryInfo {
            id: id.clone(),
            description: None,
        })
        .collect();

    let control_init_message = ControlInitMessage {
        namespace: namespace.clone(),
        factory_ids,
        factories,
    };

    let control_message = serde_json::to_string(&control_init_message).unwrap();
    let payload =
        payloader::build_from_str("connect_control_channel", &control_message, HashMap::new());

    (&*stream_clone).write_all(&payload).unwrap();
}
