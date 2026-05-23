use std::collections::HashMap;
use std::io::{self, Write};
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam::channel::Sender;
use flz_transport::service::payloader;
use flz_transport::transport::config::{get_transport_type, TransportType};
use flz_transport::transport::quic::QuicClient;
use flz_transport::transport::FlezhaStream;
use flz_transport::utility::path_helper;
use serde::Serialize;

use super::ClientResponseMessage;
use crate::executor::ExecutorMessage;
use crate::relay::registry::class_factory_ids;

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
) -> (
    thread::JoinHandle<()>,
    Arc<FlezhaStream>,
    Option<Arc<QuicClient>>,
) {
    let (handle, stream, quic_client) = open_with_retry(namespace, Some(itx), otx);
    (handle, stream, quic_client)
}

/// Reconnection entry point — no startup confirmation sent.
pub fn open_reconnect(
    namespace: String,
    otx: Arc<Mutex<Sender<ExecutorMessage>>>,
) -> (
    thread::JoinHandle<()>,
    Arc<FlezhaStream>,
    Option<Arc<QuicClient>>,
) {
    open_with_retry(namespace, None, otx)
}

fn open_with_retry(
    namespace: String,
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
    otx: Arc<Mutex<Sender<ExecutorMessage>>>,
) -> (
    thread::JoinHandle<()>,
    Arc<FlezhaStream>,
    Option<Arc<QuicClient>>,
) {
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

// reason: tuple bundles per-transport handles needed by the caller; refactoring would split the QUIC path's QuicClient out behind another wrapper
#[allow(clippy::type_complexity)]
fn try_open(
    namespace: String,
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
    otx: Arc<Mutex<Sender<ExecutorMessage>>>,
) -> Result<
    (
        thread::JoinHandle<()>,
        Arc<FlezhaStream>,
        Option<Arc<QuicClient>>,
    ),
    String,
> {
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

// reason: same wire shape as try_open; QuicClient handle is part of the return contract
#[allow(clippy::type_complexity)]
fn try_open_quic(
    namespace: String,
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
    otx: Arc<Mutex<Sender<ExecutorMessage>>>,
) -> Result<
    (
        thread::JoinHandle<()>,
        Arc<FlezhaStream>,
        Option<Arc<QuicClient>>,
    ),
    String,
> {
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

/// Maximum wait for the controller's first frame (the assign-executor-id ack)
/// after opening a control channel. A controller mid-shutdown accepts the
/// socket but rejects registration without closing it, so without this bound
/// the reader blocks forever and the executor never reaches the live
/// controller. Overridable via `FLEZHA_CONTROL_ACK_TIMEOUT_MS`.
fn connect_ack_timeout() -> Duration {
    std::env::var("FLEZHA_CONTROL_ACK_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(10_000))
}

fn sock_conn(
    namespace: String,
    stream_box: Arc<FlezhaStream>,
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
    otx: Arc<Mutex<Sender<ExecutorMessage>>>,
) -> (thread::JoinHandle<()>, Arc<FlezhaStream>) {
    let stream_clone = Arc::clone(&stream_box);

    let handle = thread::spawn(move || {
        // Bound the read until the controller acknowledges the handshake, then
        // read indefinitely. Catches a controller that accepts the socket but
        // never registers us (the warm-restart hang).
        let ack_timeout = connect_ack_timeout();
        let _ = stream_clone.set_read_timeout(Some(ack_timeout));

        if let Err(e) = send_init_msg(namespace, stream_clone.clone()) {
            if std::env::var("FLEZHA_LOG").as_deref() == Ok("debug") {
                eprintln!("[flezha] Control-channel init send failed: {}", e);
            }
            let _ = otx.lock().unwrap().send(ExecutorMessage::Reconnect);
            return;
        }

        // Send startup confirmation only on initial connection
        if let Some(itx) = itx {
            itx.lock()
                .unwrap()
                .send(ClientResponseMessage::ExecutorIdConfirmation)
                .unwrap();
        }

        let mut acked = false;

        loop {
            match payloader::extract_payload(&*stream_clone.clone()) {
                Ok((payload_id, message, metadata)) => {
                    if !acked {
                        acked = true;
                        let _ = stream_clone.set_read_timeout(None);
                    }
                    otx.lock()
                        .unwrap()
                        .send(ExecutorMessage::HandleCommandAction(
                            payload_id, message, metadata,
                        ))
                        .unwrap();
                }
                Err(e) => match e.kind() {
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut if !acked => {
                        // No controller traffic within the ack window — the
                        // registration handshake never completed. Reconnect so
                        // the executor lands on a live controller.
                        if std::env::var("FLEZHA_LOG").as_deref() == Ok("debug") {
                            eprintln!(
                                "[flezha] Controller did not acknowledge within {:?}; reconnecting",
                                ack_timeout
                            );
                        }
                        let _ = otx.lock().unwrap().send(ExecutorMessage::Reconnect);
                        break;
                    }
                    io::ErrorKind::WouldBlock
                    | io::ErrorKind::TimedOut
                    | io::ErrorKind::Interrupted => {
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
                        let _ = otx.lock().unwrap().send(ExecutorMessage::Reconnect);
                        break;
                    }
                },
            }
        }
    });

    (handle, stream_box)
}

fn send_init_msg(namespace: String, stream_clone: Arc<FlezhaStream>) -> io::Result<()> {
    let factory_ids: Vec<String> = class_factory_ids().iter().map(|s| s.to_string()).collect();
    let factories: Vec<FactoryInfo> = factory_ids
        .iter()
        .map(|id| FactoryInfo {
            id: id.clone(),
            description: None,
        })
        .collect();

    let control_init_message = ControlInitMessage {
        namespace,
        factory_ids,
        factories,
    };

    let control_message = serde_json::to_string(&control_init_message).unwrap();
    let payload =
        payloader::build_from_str("connect_control_channel", &control_message, HashMap::new());

    // A failed init write means the peer vanished between connect and write
    // (e.g. a controller that died mid-handshake); surface it so the caller
    // drives a reconnect instead of panicking the reader thread.
    (&*stream_clone).write_all(&payload)
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::os::unix::net::UnixStream;

    use crossbeam::channel::unbounded;

    use super::*;

    // fails when: the executor connects to a controller that accepts the
    // control socket but never sends the assign-executor-id ack (a controller
    // mid-shutdown / being replaced), and the SDK blocks the control reader
    // forever instead of reconnecting. This is the production warm-restart
    // hang: the executor stayed "Connected" to the draining controller, never
    // reached the new one, so no relay was ever re-registered.
    #[test]
    fn reconnects_when_controller_never_acknowledges() {
        std::env::set_var("FLEZHA_CONTROL_ACK_TIMEOUT_MS", "600");

        let (sdk_end, ctrl_end) = UnixStream::pair().unwrap();

        // Fake non-servicing controller: drain the init frame, then hold the
        // connection open and stay silent — never acknowledges the executor.
        let ctrl = thread::spawn(move || {
            let mut ctrl_end = ctrl_end;
            let mut buf = [0u8; 1024];
            let _ = ctrl_end.read(&mut buf);
            thread::sleep(Duration::from_secs(3));
        });

        let (itx, _irx) = unbounded();
        let (otx, orx) = unbounded();
        let stream = Arc::new(FlezhaStream::Unix(sdk_end));

        let _ = sock_conn(
            "test_ns".to_string(),
            stream,
            Some(Arc::new(Mutex::new(itx))),
            Arc::new(Mutex::new(otx)),
        );

        let reconnected = matches!(
            orx.recv_timeout(Duration::from_secs(2)),
            Ok(ExecutorMessage::Reconnect)
        );
        assert!(
            reconnected,
            "SDK must post Reconnect when the controller accepts the socket but never \
             acknowledges within the ack window"
        );

        ctrl.join().ok();
    }
}
