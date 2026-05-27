use std::collections::HashMap;
use std::io::{self, Write};
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam::channel::Sender;
use flz_transport::service::payloader;
use flz_transport::transport::config::{get_transport_type, TransportType};
use flz_transport::transport::liveness::{self, ControlLiveness, ReadAction};
use flz_transport::transport::quic::QuicClient;
use flz_transport::transport::FlezhaStream;
use flz_transport::utility::path_helper;
use prost::Message;
use serde::Serialize;

use super::ClientResponseMessage;
use crate::executor::ExecutorMessage;
use crate::pb::Ping;
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
                // Full jitter on the exponential backoff so a controller
                // restart that drops the whole fleet doesn't produce a
                // synchronized reconnect storm (shared with the Java SDK via
                // the same `liveness::jittered` schedule).
                let backoff = liveness::jittered(std::cmp::min(
                    Duration::from_millis(100 * 2u64.pow(attempt.min(8))),
                    max_backoff,
                ));
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

fn sock_conn(
    namespace: String,
    stream_box: Arc<FlezhaStream>,
    itx: Option<Arc<Mutex<Sender<ClientResponseMessage>>>>,
    otx: Arc<Mutex<Sender<ExecutorMessage>>>,
) -> (thread::JoinHandle<()>, Arc<FlezhaStream>) {
    let stream_clone = Arc::clone(&stream_box);

    let handle = thread::spawn(move || {
        // Shared (Rust + Java) control-channel liveness: an ack-deadline until
        // the controller's first frame, then a steady-state idle watchdog that
        // re-arms on every frame. Catches both the warm-restart "accept but
        // never ack" hang and a controller that wedges after the handshake.
        let mut live = ControlLiveness::new();
        let _ = stream_clone.set_read_timeout(Some(live.current_deadline()));

        if let Err(e) = send_init_msg(namespace, stream_clone.clone()) {
            if std::env::var("FLEZHA_LOG").as_deref() == Ok("debug") {
                eprintln!("[flezha] Control-channel init send failed: {}", e);
            }
            let _ = otx.lock().unwrap().send(ExecutorMessage::Reconnect("write_error"));
            return;
        }

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
                    live.note_frame();
                    // Re-arm the idle watchdog on every frame; a live channel
                    // is therefore never flagged while traffic is flowing.
                    let _ = stream_clone.set_read_timeout(Some(live.current_deadline()));
                    otx.lock()
                        .unwrap()
                        .send(ExecutorMessage::HandleCommandAction(
                            payload_id, message, metadata,
                        ))
                        .unwrap();
                }
                Err(e) => match live.classify_error(&e) {
                    ReadAction::Retry => continue,
                    ReadAction::Reconnect => {
                        if std::env::var("FLEZHA_LOG").as_deref() == Ok("debug") {
                            if !live.acked() {
                                eprintln!(
                                    "[flezha] Controller did not acknowledge within {:?}; reconnecting",
                                    live.current_deadline()
                                );
                            } else {
                                eprintln!(
                                    "[flezha] Control channel went silent / disconnected ({}); reconnecting",
                                    e
                                );
                            }
                        }
                        let reason = classify_reconnect_reason(&e, live.acked());
                        let _ = otx.lock().unwrap().send(ExecutorMessage::Reconnect(reason));
                        break;
                    }
                },
            }
        }
    });

    (handle, stream_box)
}

/// Build a control-channel keepalive ping frame: a bare `Ping` on the
/// dedicated `control_ping` payload. The controller echoes it (re-arming the
/// executor's steady-state idle watchdog); on a wedged controller no echo
/// arrives and the watchdog fires. Pure so it is unit-testable.
fn build_keepalive_ping() -> Vec<u8> {
    let ping = Ping {
        nonce: format!("ka-{}", nonce_suffix()),
    };
    payloader::build_from_binary(
        liveness::CONTROL_PING_PAYLOAD_ID,
        &ping.encode_to_vec(),
        HashMap::new(),
    )
}

/// Write a keepalive ping on the current control stream. Routed through the
/// coordinator (see `ExecutorMessage::SendControlKeepalive`) so control writes
/// stay single-threaded and never interleave with command replies.
pub(crate) fn send_keepalive_ping(stream: &FlezhaStream) -> io::Result<()> {
    let frame = build_keepalive_ping();
    (&*stream).write_all(&frame)
}

/// Spawn the keepalive timer. It only signals the coordinator (it never writes
/// the stream itself), so it can live for the executor's whole lifetime and
/// always pings whatever the current control stream is. Jittered so a fleet
/// does not ping in lockstep.
pub(crate) fn spawn_keepalive(otx: Arc<Mutex<Sender<ExecutorMessage>>>) -> thread::JoinHandle<()> {
    thread::spawn(move || loop {
        thread::sleep(liveness::jittered(liveness::control_keepalive_interval()));
        if otx
            .lock()
            .unwrap()
            .send(ExecutorMessage::SendControlKeepalive)
            .is_err()
        {
            // Coordinator gone (executor shutting down) — stop the timer.
            break;
        }
    })
}

/// Map a control-stream read error to one of the five `reason` strings
/// the controller-side `[:flezha, :executor, :reconnected]` telemetry
/// expects. A post-ack timeout means the keepalive watchdog tripped; a
/// pre-ack timeout means the channel was never registered. Anything
/// non-timeout is treated as a stream break.
fn classify_reconnect_reason(e: &io::Error, acked: bool) -> &'static str {
    match e.kind() {
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => {
            if acked {
                "keepalive_timeout"
            } else {
                "control_stream_broken"
            }
        }
        io::ErrorKind::UnexpectedEof
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::BrokenPipe => "control_stream_broken",
        _ => "read_error",
    }
}

fn nonce_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
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
    use std::sync::Mutex as StdMutex;

    use crossbeam::channel::unbounded;

    use super::*;

    #[test]
    fn classify_reconnect_reason_maps_kinds_to_canonical_strings() {
        let acked = true;
        let pre_ack = false;

        let timeout = io::Error::new(io::ErrorKind::TimedOut, "");
        assert_eq!(classify_reconnect_reason(&timeout, acked), "keepalive_timeout");
        assert_eq!(
            classify_reconnect_reason(&timeout, pre_ack),
            "control_stream_broken"
        );

        let would_block = io::Error::new(io::ErrorKind::WouldBlock, "");
        assert_eq!(
            classify_reconnect_reason(&would_block, acked),
            "keepalive_timeout"
        );

        let eof = io::Error::new(io::ErrorKind::UnexpectedEof, "");
        assert_eq!(classify_reconnect_reason(&eof, acked), "control_stream_broken");

        let pipe = io::Error::new(io::ErrorKind::BrokenPipe, "");
        assert_eq!(classify_reconnect_reason(&pipe, acked), "control_stream_broken");

        let reset = io::Error::new(io::ErrorKind::ConnectionReset, "");
        assert_eq!(
            classify_reconnect_reason(&reset, acked),
            "control_stream_broken"
        );

        let other = io::Error::new(io::ErrorKind::Other, "x");
        assert_eq!(classify_reconnect_reason(&other, acked), "read_error");
    }

    // The liveness env knobs are process-global, so the reader tests that set
    // them must not run concurrently.
    static ENV_LOCK: StdMutex<()> = StdMutex::new(());

    // fails when: the executor connects to a controller that accepts the
    // control socket but never sends the assign-executor-id ack (a controller
    // mid-shutdown / being replaced), and the SDK blocks the control reader
    // forever instead of reconnecting. This is the production warm-restart
    // hang: the executor stayed "Connected" to the draining controller, never
    // reached the new one, so no relay was ever re-registered.
    #[test]
    fn reconnects_when_controller_never_acknowledges() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("FLEZHA_CONTROL_ACK_TIMEOUT_MS", "600");
        std::env::set_var("FLEZHA_CONTROL_IDLE_TIMEOUT_MS", "60000");

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
            Ok(ExecutorMessage::Reconnect(_))
        );
        assert!(
            reconnected,
            "SDK must post Reconnect when the controller accepts the socket but never \
             acknowledges within the ack window"
        );

        std::env::remove_var("FLEZHA_CONTROL_ACK_TIMEOUT_MS");
        std::env::remove_var("FLEZHA_CONTROL_IDLE_TIMEOUT_MS");
        ctrl.join().ok();
    }

    // fails when: the control channel completes its handshake (first frame
    // acked) but the controller then wedges (half-open / hung) without a clean
    // close — the pre-change reader cleared its deadline (`set_read_timeout
    // (None)`) and blocked forever, so the executor sat "Connected" to a dead
    // controller. The steady-state idle watchdog must drive reconnect.
    #[test]
    fn reconnects_when_controller_wedges_after_handshake() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("FLEZHA_CONTROL_ACK_TIMEOUT_MS", "5000");
        std::env::set_var("FLEZHA_CONTROL_IDLE_TIMEOUT_MS", "500");

        let (sdk_end, ctrl_end) = UnixStream::pair().unwrap();

        // Fake controller: drain the init frame, send ONE control frame (the
        // ack), then go silent forever — the post-handshake wedge.
        let ctrl = thread::spawn(move || {
            let mut ctrl_end = ctrl_end;
            let mut buf = [0u8; 1024];
            let _ = ctrl_end.read(&mut buf);
            let ack = payloader::build_from_binary("control_command", b"\x08\x01", HashMap::new());
            let _ = ctrl_end.write_all(&ack);
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

        // First the acked frame is forwarded, then — after the idle window —
        // a Reconnect must arrive.
        let mut saw_command = false;
        let mut reconnected = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            match orx.recv_timeout(Duration::from_millis(200)) {
                Ok(ExecutorMessage::HandleCommandAction(..)) => saw_command = true,
                Ok(ExecutorMessage::Reconnect(_)) => {
                    reconnected = true;
                    break;
                }
                _ => {}
            }
        }

        assert!(saw_command, "the acked first frame must be forwarded");
        assert!(
            reconnected,
            "SDK must post Reconnect when the controller wedges after the handshake \
             (idle watchdog past FLEZHA_CONTROL_IDLE_TIMEOUT_MS)"
        );

        std::env::remove_var("FLEZHA_CONTROL_ACK_TIMEOUT_MS");
        std::env::remove_var("FLEZHA_CONTROL_IDLE_TIMEOUT_MS");
        ctrl.join().ok();
    }

    // `send_keepalive_ping` (the coordinator's write path) must put a bare
    // `Ping` on the dedicated control_ping payload, so the controller recognizes
    // it without entering command dispatch.
    #[test]
    fn keepalive_ping_is_a_bare_ping_on_control_ping_payload() {
        let (sdk_end, ctrl_end) = UnixStream::pair().unwrap();
        send_keepalive_ping(&FlezhaStream::Unix(sdk_end)).unwrap();

        let ctrl: FlezhaStream = FlezhaStream::Unix(ctrl_end);
        let (payload_id, message, _meta) = payloader::extract_payload(&ctrl).unwrap();
        assert_eq!(payload_id, liveness::CONTROL_PING_PAYLOAD_ID);
        let ping = Ping::decode(message.as_slice()).expect("ping decodes");
        assert!(ping.nonce.starts_with("ka-"));
    }
}
