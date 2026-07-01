//! Graceful-shutdown handler for the Rust SDK.
//!
//! On graceful shutdown — SIGTERM (Kubernetes pre-stop), `Drop` of the SDK
//! handle, or an explicit `disconnect()` — the SDK serializes each
//! currently-running operative's state via the same per-driver path used
//! for `MigrationSerialization` and emits one
//! `ExecutorShutdownNotification` per operative on the control channel.
//! The controller overwrites its `Overwatch.state.snapshot` with the supplied
//! binary so the next reconnecting executor picks up the executor's true
//! final state — see ship-fresh-state-on-sdk-shutdown.
//!
//! ## Timing contract
//!
//! - The SDK awaits `ExecutorShutdownAck` per operative with a **2 s
//!   per-operative timeout**, parallelised across operatives so total
//!   wait is bounded by 2 s (not 2N s).
//! - The per-operative serializer step is capped at the controller's
//!   `cross_node_request_timeout` ceiling (**10 s**). Concrete `ContinuousOperative`
//!   impls with multi-second serializers should override `bin_apply_state`
//!   to keep below that cap.
//! - On timeout the SDK exits unconditionally and SHALL NOT block its
//!   container's shutdown grace period.
//!
//! ## SIGKILL is out of scope
//!
//! `kill -9` (SIGKILL) bypasses this handler by definition. The controller's
//! `Overwatch.state.snapshot` falls back to the most recent periodic
//! snapshot, exactly as today.

use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, Once, OnceLock};
use std::thread;
use std::time::Duration;

use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;
use prost::Message;

use crate::pb;

/// Per-operative ack wait. Kubernetes default
/// `terminationGracePeriodSeconds` is 30 s; with parallel acks this
/// bounds the total ack wait at 2 s regardless of operative count.
const ACK_TIMEOUT: Duration = Duration::from_secs(2);

/// Wire-level payload_id reused from the existing controller -> SDK
/// `CommandRequest` flow. The `CommandRequest` envelope is
/// bidirectional; the controller's `ControlChannel` peeks at the oneof tag
/// to dispatch SDK-initiated variants vs `CommandResponse`s.
const SHUTDOWN_PAYLOAD_ID: &str = "control_command";

/// Returns `(instance_id, serialized_state_bytes)` per live ContinuousOperative
/// instance. The closure is responsible for walking the live registry
/// and serializing each instance via `segment_state::serialize_operative_state`.
pub type OperativeSnapshotFn = Box<dyn Fn() -> Vec<(String, Vec<u8>)> + Send + Sync>;

/// Bridge between the user-facing `Flezha` handle and the per-instance
/// runtime state required for a graceful shutdown sequence.
///
/// Bridges are registered in the global [`bridges_vec`] so the SIGTERM
/// handler can iterate every active SDK in the process. `Flezha::Drop`
/// triggers the same sequence directly.
pub struct ShutdownBridge {
    /// Latest `Arc<FlezhaStream>` for the control channel. Updated by
    /// the coordinator on initial open and on every reconnect; cleared
    /// when the connection drops.
    control_stream: Mutex<Option<Arc<FlezhaStream>>>,

    /// Closure that returns a point-in-time snapshot of currently-running
    /// operatives. Populated by the coordinator at startup; the closure
    /// captures an `Arc` to the backend's registry so the bridge stays
    /// decoupled from the dispatcher implementation.
    snapshot_fn: Mutex<Option<OperativeSnapshotFn>>,

    /// Single-shot guard: a graceful shutdown runs at most once per
    /// bridge so SIGTERM + `Drop` racing each other does not double-emit
    /// notifications.
    shutdown_started: AtomicBool,
}

impl ShutdownBridge {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            control_stream: Mutex::new(None),
            snapshot_fn: Mutex::new(None),
            shutdown_started: AtomicBool::new(false),
        })
    }

    pub fn set_control_stream(&self, stream: Option<Arc<FlezhaStream>>) {
        *self.control_stream.lock().unwrap() = stream;
    }

    pub fn set_snapshot_fn(&self, snapshot_fn: OperativeSnapshotFn) {
        *self.snapshot_fn.lock().unwrap() = Some(snapshot_fn);
    }

    /// Run the graceful-shutdown sequence. Idempotent across SIGTERM +
    /// `Drop`: the second caller observes
    /// `shutdown_started.swap(true) == true` and returns early.
    pub fn run_graceful_shutdown(self: &Arc<Self>) {
        if self.shutdown_started.swap(true, Ordering::SeqCst) {
            return;
        }

        let stream = match self.control_stream.lock().unwrap().clone() {
            Some(s) => s,
            None => {
                // No control channel attached: e.g. the SDK was dropped
                // before it ever connected, or the connection is in the
                // middle of a reconnect cycle. Nothing to ship.
                return;
            }
        };

        let operatives = match self.snapshot_fn.lock().unwrap().as_ref() {
            Some(f) => f(),
            None => return,
        };

        if operatives.is_empty() {
            return;
        }

        let mut wait_handles = Vec::with_capacity(operatives.len());

        for (operative_id, serialized_state) in operatives {
            let reference_id = format!("shutdown-{}-{}", operative_id, crate::util::nonce_suffix());

            let request = pb::CommandRequest {
                reference_id: reference_id.clone(),
                command: Some(pb::command_request::Command::ExecutorShutdownNotification(
                    pb::ExecutorShutdownNotification {
                        operative_id: operative_id.clone(),
                        serialized_state,
                    },
                )),
            };

            let encoded = request.encode_to_vec();
            let payload =
                payloader::build_from_binary(SHUTDOWN_PAYLOAD_ID, &encoded, HashMap::new());

            let rx = register_pending_ack(reference_id.clone());

            if let Err(e) = (&*stream).write_all(&payload) {
                eprintln!(
                    "[flezha] Failed to write ExecutorShutdownNotification for {}: {}",
                    operative_id, e
                );
                drop_pending_ack(&reference_id);
                continue;
            }

            let operative_id_for_thread = operative_id;
            let reference_id_for_thread = reference_id;
            wait_handles.push(thread::spawn(move || match rx.recv_timeout(ACK_TIMEOUT) {
                Ok(()) => {}
                Err(_) => {
                    eprintln!(
                        "[flezha] ExecutorShutdownAck timeout for {}",
                        operative_id_for_thread
                    );
                    drop_pending_ack(&reference_id_for_thread);
                }
            }));
        }

        for handle in wait_handles {
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Process-global registries.
//
// `PENDING_ACKS` lets the controller->SDK `ExecutorShutdownAck` command handler
// (in `command::executor_shutdown_ack`) signal a per-operative wait
// channel without needing access to the originating bridge — handlers
// only see `&mut ExecutorNode`, so a global keyed by reference_id is
// the simplest plumbing.
//
// `BRIDGES` lets a single SIGTERM handler thread drive shutdown across
// every active SDK in the process. The Flezha public API allows multiple
// `new()` calls; each registers its bridge here.
// ---------------------------------------------------------------------------

fn pending_acks_map() -> &'static Mutex<HashMap<String, mpsc::SyncSender<()>>> {
    static PENDING_ACKS: OnceLock<Mutex<HashMap<String, mpsc::SyncSender<()>>>> = OnceLock::new();
    PENDING_ACKS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn register_pending_ack(reference_id: String) -> mpsc::Receiver<()> {
    let (tx, rx) = mpsc::sync_channel(1);
    pending_acks_map().lock().unwrap().insert(reference_id, tx);
    rx
}

fn drop_pending_ack(reference_id: &str) {
    pending_acks_map().lock().unwrap().remove(reference_id);
}

/// Called from the controller->SDK `ExecutorShutdownAck` command handler. Wakes
/// the per-operative wait channel registered by `run_graceful_shutdown`.
/// A missing entry is logged and ignored — this happens after the wait
/// already timed out and dropped its registration.
pub fn signal_ack(reference_id: &str) {
    let entry = pending_acks_map().lock().unwrap().remove(reference_id);
    match entry {
        Some(tx) => {
            let _ = tx.send(());
        }
        None => {
            eprintln!(
                "[flezha] ExecutorShutdownAck for unknown reference_id {} — \
                 either timed out already or never registered.",
                reference_id
            );
        }
    }
}

fn bridges_vec() -> &'static Mutex<Vec<Arc<ShutdownBridge>>> {
    static BRIDGES: OnceLock<Mutex<Vec<Arc<ShutdownBridge>>>> = OnceLock::new();
    BRIDGES.get_or_init(|| Mutex::new(Vec::new()))
}

pub fn register_bridge(bridge: Arc<ShutdownBridge>) {
    bridges_vec().lock().unwrap().push(bridge);
    install_sigterm_handler();
}

/// Installs a one-shot SIGTERM handler on the first call. Subsequent
/// calls are no-ops via `std::sync::Once`. The handler iterates every
/// registered bridge, runs graceful shutdown synchronously, then exits
/// the process — Kubernetes' grace period is the outer bound.
fn install_sigterm_handler() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        match signal_hook::iterator::Signals::new([signal_hook::consts::SIGTERM]) {
            Ok(mut signals) => {
                thread::spawn(move || {
                    if signals.forever().next().is_some() {
                        eprintln!("[flezha] SIGTERM received; running graceful shutdown");
                        let bridges: Vec<Arc<ShutdownBridge>> =
                            bridges_vec().lock().unwrap().clone();
                        for bridge in &bridges {
                            bridge.run_graceful_shutdown();
                        }
                        std::process::exit(0);
                    }
                });
            }
            Err(e) => {
                eprintln!(
                    "[flezha] Failed to install SIGTERM handler ({}); graceful \
                     shutdown will rely on Drop only.",
                    e
                );
            }
        }
    });
}

#[cfg(test)]
mod tests {
    //! Bridge mechanics tests. Drive `run_graceful_shutdown` against a
    //! `UnixStream::pair()` standing in for the controller's control channel,
    //! then decode the captured frame and assert protocol shape.
    //!
    //! The full SIGTERM-from-Kubernetes path is exercised in the
    //! dev-compose state-freshness scenario (§6 of
    //! ship-fresh-state-on-sdk-shutdown); here we cover the unit
    //! contract: notification correctly framed + bounded ack wait.
    use std::io::Read;
    use std::os::unix::net::UnixStream;
    use std::time::Instant;

    use prost::Message;

    use super::*;
    use crate::pb;

    fn build_bridge_with_operative(
        operative_id: &str,
        snapshot: Vec<u8>,
    ) -> (Arc<ShutdownBridge>, UnixStream) {
        let (sdk_side, controller_side) = UnixStream::pair().expect("UnixStream::pair");

        let bridge = ShutdownBridge::new();
        bridge.set_control_stream(Some(Arc::new(FlezhaStream::Unix(sdk_side))));

        let pid = operative_id.to_string();
        let snap = snapshot.clone();
        bridge.set_snapshot_fn(Box::new(move || vec![(pid.clone(), snap.clone())]));

        (bridge, controller_side)
    }

    /// Decode a Payloader-framed payload from raw bytes captured off the
    /// control channel. Mirrors the format produced by
    /// `flz_transport::service::payloader::build_from_binary` — 32-byte
    /// padded id, 4-byte payload size, 4-byte message size, message,
    /// 2-byte metadata size, metadata.
    fn decode_frame(buf: &[u8]) -> (String, Vec<u8>) {
        assert!(buf.len() >= 36, "frame must have header");
        let id_bytes = &buf[..32];
        let payload_size = u32::from_be_bytes([buf[32], buf[33], buf[34], buf[35]]) as usize;
        let body = &buf[36..36 + payload_size];
        let message_size = u32::from_be_bytes([body[0], body[1], body[2], body[3]]) as usize;
        let message = &body[4..4 + message_size];

        let id = std::str::from_utf8(id_bytes)
            .expect("payload id utf-8")
            .trim_end_matches(' ')
            .to_string();
        (id, message.to_vec())
    }

    #[test]
    fn graceful_shutdown_emits_framed_executor_shutdown_notification() {
        let snapshot = b"final-state-bytes".to_vec();
        let (bridge, mut controller_side) = build_bridge_with_operative("p1", snapshot.clone());

        let bridge_for_run = Arc::clone(&bridge);
        // run_graceful_shutdown blocks waiting for an ack we never send;
        // off-thread it so we can drive the controller side and bound the wait.
        let run_handle = std::thread::spawn(move || bridge_for_run.run_graceful_shutdown());

        // Read the frame the SDK emitted. UnixStream is blocking — the
        // SDK writes the notification before sleeping on the ack channel,
        // so a single read() pulls the full frame.
        let mut buf = vec![0u8; 4096];
        let n = controller_side.read(&mut buf).expect("read shutdown frame");
        buf.truncate(n);

        let (payload_id, message) = decode_frame(&buf);
        assert_eq!(payload_id, "control_command");

        let request = pb::CommandRequest::decode(message.as_slice())
            .expect("decode CommandRequest from shutdown frame");

        let notif = match request.command.expect("oneof command set") {
            pb::command_request::Command::ExecutorShutdownNotification(n) => n,
            other => panic!("expected ExecutorShutdownNotification, got {:?}", other),
        };

        assert_eq!(notif.operative_id, "p1");
        assert_eq!(notif.serialized_state, snapshot);
        assert!(!request.reference_id.is_empty());

        // Now signal the ack so the wait thread releases. Closing the
        // socket on the controller side without acking would also work but
        // we exercise the happy path explicitly.
        signal_ack(&request.reference_id);
        run_handle.join().expect("shutdown thread joins");
    }

    #[test]
    fn graceful_shutdown_returns_within_2s_when_ack_never_arrives() {
        let (bridge, _controller_side) =
            build_bridge_with_operative("p_timeout", b"unused".to_vec());

        let started = Instant::now();
        bridge.run_graceful_shutdown();
        let elapsed = started.elapsed();

        // Timeout is 2 s per operative; allow a small slack for thread
        // spawn + join. The Kubernetes default grace period is 30 s, so
        // anything significantly above 2.5 s here would mean we have a
        // bug that could blow past the grace period.
        assert!(
            elapsed < Duration::from_millis(2500),
            "graceful shutdown should bound wait at 2s; took {:?}",
            elapsed
        );
        assert!(
            elapsed >= Duration::from_millis(1900),
            "graceful shutdown should wait the full 2s before timing out; took {:?}",
            elapsed
        );
    }

    #[test]
    fn signal_ack_for_unknown_reference_does_not_panic() {
        // Matches the post-timeout cleanup race: the wait thread already
        // dropped its registration; a late ack arrives and finds nothing.
        // Must not panic.
        signal_ack("never-registered-ref");
    }

    #[test]
    fn run_graceful_shutdown_is_idempotent() {
        let (bridge, _controller_side) = build_bridge_with_operative("p_idem", b"x".to_vec());

        // First run — exercises the timeout path. Second run sees
        // shutdown_started == true and returns immediately.
        bridge.run_graceful_shutdown();
        let started = Instant::now();
        bridge.run_graceful_shutdown();
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "second run must be a no-op"
        );
    }
}
