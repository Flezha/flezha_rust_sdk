use super::CommandAction;
use crate::pb::ConnectSocket;

use std::collections::HashMap;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::thread;

use crate::compute_engine::ComputeEngineNode;
use crate::processor::ProcessorInput;
use flz_transport::service::payloader;
use flz_transport::transport::config::{get_transport_type, TransportType};
use flz_transport::transport::protocol;
use flz_transport::transport::FlezhaStream;

use flz_transport::utility::path_helper;

#[cfg(not(feature = "fast-dispatch"))]
use {
    crate::compute_engine::ComputeEngineMessage,
    crossbeam::channel::Sender,
};

#[cfg(feature = "fast-dispatch")]
use crate::compute_engine::fast::FastDispatchHandle;

impl CommandAction for ConnectSocket {
    fn handle(
        &self,
        compute_engine: &mut ComputeEngineNode,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        match get_transport_type() {
            TransportType::Uds => handle_uds(self, compute_engine),
            TransportType::Quic => handle_quic(self, compute_engine),
        }
    }
}

fn handle_uds(cmd: &ConnectSocket, compute_engine: &mut ComputeEngineNode) -> (String, Vec<u8>) {
    let io_streams = compute_engine.get_io_streams();
    let compute_engine_id = compute_engine.get_compute_engine_id();

    // Pair each primary with its secondary explicitly. Agent→SDK reads on
    // primary; SDK→agent writes on secondary. Rust used to write back on
    // primary, which put inbound reads + outbound writes on the same FD and
    // hurt throughput ~4× vs the Java SDK (which writes on the secondary).
    let mut stream_pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)> = Vec::new();

    for key in 0..cmd.sockets_count {
        let primary = connect_unix(&compute_engine_id, "primary".to_string());
        let secondary = connect_unix(&compute_engine_id, "secondary".to_string());

        let secondary_for_store = FlezhaStream::Unix(secondary.try_clone().unwrap());
        io_streams.write().unwrap().insert(key, secondary_for_store);

        let secondary_for_write = Arc::new(FlezhaStream::Unix(secondary));
        stream_pairs.push((FlezhaStream::Unix(primary), secondary_for_write));
    }

    spawn_listener_uds(compute_engine, stream_pairs);
    ("connect_sockets".to_string(), Vec::new())
}

fn handle_quic(cmd: &ConnectSocket, compute_engine: &mut ComputeEngineNode) -> (String, Vec<u8>) {
    let io_streams = compute_engine.get_io_streams();

    let quic_client = compute_engine
        .get_quic_client()
        .expect("QUIC client not available");

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

    spawn_listener_quic(compute_engine, stream_pairs);
    ("connect_sockets".to_string(), Vec::new())
}

// ---------------------------------------------------------------------------
// Listener spawn — legacy path (coordinator channel) and fast path (mailbox).
// ---------------------------------------------------------------------------

#[cfg(not(feature = "fast-dispatch"))]
fn spawn_listener_uds(
    compute_engine: &ComputeEngineNode,
    pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
) {
    let ctx = compute_engine.get_compute_engine_channel();
    let ctx_clone = ctx.lock().unwrap().clone();
    let _ = thread::spawn(move || listen_incoming_uds_legacy(pairs, ctx_clone));
}

#[cfg(not(feature = "fast-dispatch"))]
fn spawn_listener_quic(
    compute_engine: &ComputeEngineNode,
    pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
) {
    let ctx = compute_engine.get_compute_engine_channel();
    let ctx_clone = ctx.lock().unwrap().clone();
    let _ = thread::spawn(move || listen_incoming_quic_legacy(pairs, ctx_clone));
}

#[cfg(feature = "fast-dispatch")]
fn spawn_listener_uds(
    compute_engine: &ComputeEngineNode,
    pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
) {
    let handle = compute_engine.fast_handle();
    let _ = thread::spawn(move || listen_incoming_uds_fast(pairs, handle));
}

#[cfg(feature = "fast-dispatch")]
fn spawn_listener_quic(
    compute_engine: &ComputeEngineNode,
    pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
) {
    let handle = compute_engine.fast_handle();
    let _ = thread::spawn(move || listen_incoming_quic_fast(pairs, handle));
}

// ---------------------------------------------------------------------------
// Legacy listen loops: publish to the coordinator via `ComputeEngineMessage`.
// ---------------------------------------------------------------------------

#[cfg(not(feature = "fast-dispatch"))]
fn listen_incoming_uds_legacy(
    pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
    ctx: Sender<ComputeEngineMessage>,
) {
    let mut handles = Vec::new();
    for (primary, secondary) in pairs {
        let ctx = ctx.clone();
        let primary = Arc::new(primary);
        let handle = thread::spawn(move || loop {
            match payloader::extract_payload(&mut &*primary) {
                Ok((processor_id, payload, metadata)) => {
                    let input = parse_input(payload, &metadata);
                    // Ship the SECONDARY stream to the executor so writes
                    // back to the agent don't contend with the primary's
                    // read side.
                    ctx.send(ComputeEngineMessage::WorkerCheckOut(
                        processor_id,
                        input,
                        Arc::clone(&secondary),
                    ))
                    .expect("Failed to send message to coordinator");
                }
                Err(_e) => break,
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        let _ = handle.join();
    }
}

#[cfg(not(feature = "fast-dispatch"))]
fn listen_incoming_quic_legacy(
    mut pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
    ctx: Sender<ComputeEngineMessage>,
) {
    let mut handles = Vec::new();
    for (primary, secondary) in pairs.drain(..) {
        let ctx = ctx.clone();
        let primary = Arc::new(primary);
        let handle = thread::spawn(move || loop {
            match payloader::extract_payload(&mut &*primary) {
                Ok((processor_id, payload, metadata)) => {
                    let input = parse_input(payload, &metadata);
                    ctx.send(ComputeEngineMessage::WorkerCheckOut(
                        processor_id,
                        input,
                        Arc::clone(&secondary),
                    ))
                    .expect("Failed to send message to coordinator");
                }
                Err(_e) => break,
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        let _ = handle.join();
    }
}

// ---------------------------------------------------------------------------
// Fast-dispatch listen loops: push directly into per-processor mailboxes.
// ---------------------------------------------------------------------------

#[cfg(feature = "fast-dispatch")]
fn listen_incoming_uds_fast(
    pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
    handle: FastDispatchHandle,
) {
    let mut handles = Vec::new();
    for (primary, secondary) in pairs {
        let handle = handle.clone();
        let primary = Arc::new(primary);
        let h = thread::spawn(move || {
            // Buffered reader collapses the 3 read syscalls per message
            // (32-byte id + 4-byte size + payload) into single-buffer refills.
            // This is the #1 reason the Rust SDK under-performed Java: Java's
            // JNI reader uses an 8KB buf per syscall; Rust's payloader did
            // 3 small syscalls per frame. At 1M msgs that's the difference
            // between ~3M read syscalls and ~few-thousand.
            let mut reader = std::io::BufReader::with_capacity(64 * 1024, &*primary);
            loop {
                match payloader::extract_payload(&mut reader) {
                    Ok((processor_id, payload, metadata)) => {
                        let input = parse_input(payload, &metadata);
                        handle.dispatch(processor_id, input, Arc::clone(&secondary));
                    }
                    Err(_e) => break,
                }
            }
        });
        handles.push(h);
    }
    for h in handles {
        let _ = h.join();
    }
}

#[cfg(feature = "fast-dispatch")]
fn listen_incoming_quic_fast(
    mut pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
    handle: FastDispatchHandle,
) {
    let mut handles = Vec::new();
    for (primary, secondary) in pairs.drain(..) {
        let handle = handle.clone();
        let primary = Arc::new(primary);
        let h = thread::spawn(move || {
            let mut reader = std::io::BufReader::with_capacity(64 * 1024, &*primary);
            loop {
                match payloader::extract_payload(&mut reader) {
                    Ok((processor_id, payload, metadata)) => {
                        let input = parse_input(payload, &metadata);
                        handle.dispatch(processor_id, input, Arc::clone(&secondary));
                    }
                    Err(_e) => break,
                }
            }
        });
        handles.push(h);
    }
    for h in handles {
        let _ = h.join();
    }
}

// ---------------------------------------------------------------------------
// Shared helpers.
// ---------------------------------------------------------------------------

fn connect_unix(compute_engine_id: &String, stream_type: String) -> UnixStream {
    let socket_path = path_helper::stream_socket_path(compute_engine_id, stream_type);

    UnixStream::connect(socket_path.clone()).expect(&format!(
        "Failed to connect to the socket: {}",
        socket_path.clone()
    ))
}

fn parse_input(payload: Vec<u8>, metadata: &HashMap<String, String>) -> ProcessorInput {
    // Fast path: `input_type` either absent (binary, the default) or "1"/"2".
    // Avoids an owned-String default on the hot path.
    let input_type: &str = metadata.get("input_type").map(String::as_str).unwrap_or("1");

    match input_type {
        "1" => ProcessorInput::Binary(payload),
        // `String::from_utf8` reuses the Vec<u8>'s allocation as the String's
        // backing store — zero-copy vs the old `from_utf8(&p).unwrap().to_string()`
        // which validated AND allocated a fresh String.
        "2" => ProcessorInput::Text(
            String::from_utf8(payload).expect("input_type=2 but payload is not valid UTF-8"),
        ),
        err_type => panic!("Invalid input type {}", err_type),
    }
}
