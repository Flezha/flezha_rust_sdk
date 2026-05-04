use super::CommandAction;
use crate::pb::ConnectSocket;

use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::thread;

use crate::compute_engine::ComputeEngineNode;
use flz_transport::service::payloader;
use flz_transport::transport::config::{get_transport_type, TransportType};
use flz_transport::transport::protocol;
use flz_transport::transport::FlezhaStream;
use flz_transport::utility::path_helper;

use crate::compute_engine::fast::RelayDispatchHandle;

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

    // Pair each primary with its secondary explicitly. Agent→SDK reads
    // on primary; SDK→agent writes on secondary. Putting reads + writes
    // on the same FD hurts throughput ~4× (Java SDK writes on the
    // secondary; matching that here keeps the throughput floor).
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

fn spawn_listener_uds(
    compute_engine: &ComputeEngineNode,
    pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
) {
    let handle = compute_engine.relay_dispatch_handle();
    let _ = thread::spawn(move || listen_incoming_uds(pairs, handle));
}

fn spawn_listener_quic(
    compute_engine: &ComputeEngineNode,
    pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
) {
    let handle = compute_engine.relay_dispatch_handle();
    let _ = thread::spawn(move || listen_incoming_quic(pairs, handle));
}

fn listen_incoming_uds(
    pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
    handle: RelayDispatchHandle,
) {
    let mut handles = Vec::new();
    for (primary, secondary) in pairs {
        let handle = handle.clone();
        let primary = Arc::new(primary);
        let h = thread::spawn(move || {
            let mut reader = std::io::BufReader::with_capacity(64 * 1024, &*primary);
            loop {
                match payloader::extract_payload(&mut reader) {
                    Ok((instance_id, payload, metadata)) => {
                        handle.dispatch(
                            &instance_id,
                            payload,
                            &metadata,
                            Arc::clone(&secondary),
                        );
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

fn listen_incoming_quic(
    mut pairs: Vec<(FlezhaStream, Arc<FlezhaStream>)>,
    handle: RelayDispatchHandle,
) {
    let mut handles = Vec::new();
    for (primary, secondary) in pairs.drain(..) {
        let handle = handle.clone();
        let primary = Arc::new(primary);
        let h = thread::spawn(move || {
            let mut reader = std::io::BufReader::with_capacity(64 * 1024, &*primary);
            loop {
                match payloader::extract_payload(&mut reader) {
                    Ok((instance_id, payload, metadata)) => {
                        handle.dispatch(
                            &instance_id,
                            payload,
                            &metadata,
                            Arc::clone(&secondary),
                        );
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

fn connect_unix(compute_engine_id: &String, stream_type: String) -> UnixStream {
    let socket_path = path_helper::stream_socket_path(compute_engine_id, stream_type);

    UnixStream::connect(socket_path.clone()).expect(&format!(
        "Failed to connect to the socket: {}",
        socket_path.clone()
    ))
}

use std::io::Write;
