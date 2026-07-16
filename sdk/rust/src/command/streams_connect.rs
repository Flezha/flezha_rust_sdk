use std::collections::HashMap;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
use std::thread;

use flz_transport::service::payloader;
use flz_transport::transport::config::{get_transport_type, TransportType};
use flz_transport::transport::{protocol, FlezhaStream};
use flz_transport::utility::path_helper;

use super::CommandAction;
use crate::pb::ConnectSocket;
use crate::runner::fast::OperativeDispatchHandle;
use crate::runner::RunnerNode;
use crate::segment::flusher::{SendError, SendFrameFn};
use crate::segment::segment_state_sink;

impl CommandAction for ConnectSocket {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        match get_transport_type() {
            TransportType::Uds => handle_uds(self, runner),
            TransportType::Quic => handle_quic(self, runner),
        }
    }
}

fn handle_uds(cmd: &ConnectSocket, runner: &mut RunnerNode) -> (String, Vec<u8>) {
    let io_streams = runner.get_io_streams();
    let runner_id = runner.get_runner_id();

    // Pair each primary with its secondary explicitly. Controller→SDK reads
    // on primary; SDK→Controller writes on secondary. Putting reads + writes
    // on the same FD hurts throughput ~4× (Java SDK writes on the
    // secondary; matching that here keeps the throughput floor).
    let mut stream_pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)> = Vec::new();

    for key in 0..cmd.sockets_count {
        let primary = connect_unix(&runner_id, "primary".to_string());
        let secondary = connect_unix(&runner_id, "secondary".to_string());

        let secondary_for_store = FlezhaStream::Unix(secondary.try_clone().unwrap());
        io_streams.write().unwrap().insert(key, secondary_for_store);

        let secondary_for_write = Arc::new(FlezhaStream::Unix(secondary));
        stream_pairs.push((FlezhaStream::Unix(primary), secondary_for_write));
    }

    install_segment_state_sink_from_pairs(&stream_pairs);
    spawn_listener_uds(runner, stream_pairs);
    ("connect_sockets".to_string(), Vec::new())
}

fn handle_quic(cmd: &ConnectSocket, runner: &mut RunnerNode) -> (String, Vec<u8>) {
    let io_streams = runner.get_io_streams();

    let quic_client = runner.get_quic_client().expect("QUIC client not available");

    let mut stream_pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)> = Vec::new();

    for key in 0..cmd.sockets_count {
        let mut primary = quic_client
            .open_bidi_stream()
            .expect("Failed to open primary QUIC stream");
        primary
            .write_all(&[protocol::STREAM_TYPE_PRIMARY, key as u8])
            .expect("Failed to send primary stream type");

        let mut secondary = quic_client
            .open_bidi_stream()
            .expect("Failed to open secondary QUIC stream");
        secondary
            .write_all(&[protocol::STREAM_TYPE_SECONDARY, key as u8])
            .expect("Failed to send secondary stream type");

        let secondary_for_store = FlezhaStream::Quic(secondary.try_clone());
        io_streams.write().unwrap().insert(key, secondary_for_store);

        let secondary_for_write = Arc::new(FlezhaStream::Quic(secondary));
        stream_pairs.push((FlezhaStream::Quic(primary), secondary_for_write));
    }

    install_segment_state_sink_from_pairs(&stream_pairs);
    spawn_listener_quic(runner, stream_pairs);
    ("connect_sockets".to_string(), Vec::new())
}

/// Install the global `segment_state_sink` wrapping the FIRST
/// secondary stream from the freshly-connected pairs. The flusher uses
/// this sink to ship `segment_state_change` and
/// `segment_state_snapshot` frames; one secondary is enough because
/// the WAL is single-writer per segment and the controller serializes the
/// per-Segment writes server-side.
fn install_segment_state_sink_from_pairs(pairs: &[(FlezhaStream, Arc<FlezhaStream>)]) {
    // Credit never carries across sessions: drop the old window (waking any
    // parked sender into the legacy path) until the controller re-offers
    // `CreditWindowInit` for the fresh attach.
    crate::segment::credit::global().disarm();
    if let Some((_, secondary)) = pairs.first() {
        // Mirror the secondary stream for the reaper's one-shot delete send.
        segment_state_sink::set_secondary_stream(Some(Arc::clone(secondary)));
        let sink_stream = Arc::clone(secondary);
        // The Mutex serializes writes from the single flusher thread —
        // belt-and-braces given that the flusher has only one writer.
        let writer = Arc::new(Mutex::new(sink_stream));
        let send_fn: SendFrameFn = Arc::new(
            move |payload_id: &str, frame: &[u8]| -> Result<(), SendError> {
                let payload = payloader::build_from_binary(payload_id, frame, HashMap::new());
                let stream = writer.lock().map_err(|e| SendError::Other(e.to_string()))?;
                (&**stream)
                    .write_all(&payload)
                    .map_err(|e| SendError::TransportDown(e.to_string()))
            },
        );
        segment_state_sink::install_global_sink(send_fn);
    }
}

fn spawn_listener_uds(runner: &RunnerNode, pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>) {
    let handle = runner.operative_dispatch_handle();
    let _ = thread::spawn(move || listen_incoming_uds(pairs, handle));
}

fn spawn_listener_quic(runner: &RunnerNode, pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>) {
    let handle = runner.operative_dispatch_handle();
    let _ = thread::spawn(move || listen_incoming_quic(pairs, handle));
}

fn run_data_reader(
    transport: &'static str,
    key: u32,
    primary: Arc<FlezhaStream>,
    secondary: Arc<FlezhaStream>,
    handle: OperativeDispatchHandle,
) {
    let mut reader = std::io::BufReader::with_capacity(64 * 1024, &*primary);
    let mut frames: u64 = 0;
    let exit_reason;
    loop {
        match payloader::extract_payload(&mut reader) {
            Ok((instance_id, payload, metadata)) => {
                frames += 1;
                handle.dispatch(&instance_id, payload, &metadata, Arc::clone(&secondary));
            }
            Err(e) => {
                exit_reason = e;
                break;
            }
        }
    }
    tracing::warn!(
        target: "flezha::transport",
        transport, stream_key = key, frames_seen = frames,
        kind = ?exit_reason.kind(),
        "data stream reader exiting: {}",
        exit_reason
    );
}

fn listen_incoming_uds(
    pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
    handle: OperativeDispatchHandle,
) {
    let mut handles = Vec::new();
    for (key, (primary, secondary)) in pairs.into_iter().enumerate() {
        let handle = handle.clone();
        let primary = Arc::new(primary);
        let h = thread::spawn(move || {
            run_data_reader("uds", key as u32, primary, secondary, handle);
        });
        handles.push(h);
    }
    for h in handles {
        let _ = h.join();
    }
}

fn listen_incoming_quic(
    mut pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
    handle: OperativeDispatchHandle,
) {
    let mut handles = Vec::new();
    for (key, (primary, secondary)) in pairs.drain(..).enumerate() {
        let handle = handle.clone();
        let primary = Arc::new(primary);
        let h = thread::spawn(move || {
            run_data_reader("quic", key as u32, primary, secondary, handle);
        });
        handles.push(h);
    }
    for h in handles {
        let _ = h.join();
    }
}

fn connect_unix(runner_id: &str, stream_type: String) -> UnixStream {
    let socket_path = path_helper::stream_socket_path(runner_id, stream_type);

    // The controller binds the per-runner data sockets immediately before
    // sending ConnectSocket, but bind+connect can race on busy CI. Retry with
    // bounded backoff (up to ~3s) before giving up; matches the control
    // channel's own open_with_retry pattern in `runner::control_unit`.
    let mut attempt = 0u32;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        match UnixStream::connect(&socket_path) {
            Ok(s) => return s,
            Err(e) => {
                if std::time::Instant::now() >= deadline {
                    panic!(
                        "Failed to connect to the socket {} after {} attempts: {}",
                        socket_path, attempt, e
                    );
                }
                attempt += 1;
                let backoff = std::cmp::min(
                    std::time::Duration::from_millis(10 * 2u64.pow(attempt.min(6))),
                    std::time::Duration::from_millis(500),
                );
                std::thread::sleep(backoff);
            }
        }
    }
}
