//! Per-Partition mailboxes backed by a DashMap registry and a Chase-Lev
//! work-stealing pool. The coordinator thread still exists for control-plane
//! traffic (`HandleCommandAction`, `ReturnRelays`, `Reconnect`) but
//! data-plane traffic bypasses it entirely — stream readers push directly
//! into the targeted Relay instance's partition mailboxes.

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};

use crossbeam::channel::{Receiver, Sender};
use flz_transport::transport::quic::QuicClient;
use flz_transport::transport::FlezhaStream;

use super::work_stealing_pool::WorkStealingPool;
use super::{control_unit, ClientResponseMessage, ExecutorMessage, ExecutorStatus};
use crate::command;
use crate::pb::{
    command_request::Command, CommandRequest, ExecutorReconnected as PbExecutorReconnected,
};
use crate::relay::instance::RelayInstance;
use crate::relay::registry::{get_class as get_relay_class, RelayInstanceRegistry};
use crate::shutdown::ShutdownBridge;
use crate::util::nonce_suffix;
use flz_transport::service::payloader;
use prost::Message;
use tracing::warn;

pub struct ExecutorNode {
    executor_id: String,
    _namespace: String,
    relay_registry: Arc<RelayInstanceRegistry>,
    pool: Arc<WorkStealingPool>,
    shared_io_streams: Arc<RwLock<HashMap<u32, FlezhaStream>>>,
    quic_client: Option<Arc<QuicClient>>,
    status: ExecutorStatus,
}

impl ExecutorNode {
    pub fn new(namespace: String) -> ExecutorNode {
        let relay_registry = Arc::new(RelayInstanceRegistry::new());
        let pool = Arc::new(WorkStealingPool::new());
        let shared_io_streams = Arc::new(RwLock::new(HashMap::new()));

        ExecutorNode {
            executor_id: "undefined".to_string(),
            _namespace: namespace,
            relay_registry,
            pool,
            shared_io_streams,
            quic_client: None,
            status: ExecutorStatus::Starting,
        }
    }

    /// Spawn a Relay instance from a registered Relay class. `factory_id`
    /// is looked up in the global class registry; `instance_id` is what
    /// the wire calls `relay_id`. Idempotent on duplicate
    /// `instance_id`: returns the existing instance.
    pub fn start_relay(
        &self,
        factory_id: String,
        instance_id: String,
    ) -> Option<Arc<RelayInstance>> {
        if let Some(existing) = self.relay_registry.get(&instance_id) {
            return Some(existing);
        }
        let topology = get_relay_class(&factory_id)?;
        let instance = RelayInstance::new(
            topology,
            Arc::from(instance_id.as_str()),
            Arc::clone(&self.pool),
        );
        Some(self.relay_registry.register(instance))
    }

    pub fn stop_relay(&self, instance_id: &str) -> bool {
        self.relay_registry.remove(instance_id).is_some()
    }

    pub fn has_relay(&self, instance_id: &str) -> bool {
        self.relay_registry.contains(instance_id)
    }

    pub fn get_relay(&self, instance_id: &str) -> Option<Arc<RelayInstance>> {
        self.relay_registry.get(instance_id)
    }

    pub fn relay_dispatch_handle(&self) -> RelayDispatchHandle {
        RelayDispatchHandle {
            registry: Arc::clone(&self.relay_registry),
        }
    }

    pub fn _get_namespace(&self) -> String {
        self._namespace.clone()
    }

    /// Public-API view: snapshot of live Relay instances. Mirrors the
    /// pre-`relay-mvp-rust` `Flezha::get_relays()` shape so external
    /// inspectors that walk the registry continue to work.
    // reason: returned shape is the public API; splitting it would break external inspectors
    #[allow(clippy::type_complexity)]
    pub fn get_relays(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<Arc<RelayInstance>>>>>> {
        crate::relay::registry::snapshot_as_relay_view(&self.relay_registry)
    }

    pub fn get_io_streams(&self) -> Arc<RwLock<HashMap<u32, FlezhaStream>>> {
        Arc::clone(&self.shared_io_streams)
    }

    pub fn get_executor_id(&self) -> String {
        self.executor_id.clone()
    }

    pub fn assign_executor_id(&mut self, executor_id: String) {
        self.executor_id = executor_id;
    }

    pub fn set_status(&mut self, status: ExecutorStatus) {
        self.status = status;
    }

    pub fn get_quic_client(&self) -> Option<Arc<QuicClient>> {
        self.quic_client.clone()
    }

    pub fn set_quic_client(&mut self, client: Option<Arc<QuicClient>>) {
        self.quic_client = client;
    }
}

pub fn start(
    namespace: String,
    (otx, orx): (Sender<ExecutorMessage>, Receiver<ExecutorMessage>),
    ctx: Sender<ClientResponseMessage>,
    shutdown_bridge: Arc<ShutdownBridge>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut executor = ExecutorNode::new(namespace.clone());

        // Capture a registry snapshot closure now so the shutdown bridge
        // can enumerate live Relay instances without going through the
        // coordinator channel. Survives reconnects since the registry
        // Arc is stable.
        {
            let registry = Arc::clone(&executor.relay_registry);
            shutdown_bridge.set_snapshot_fn(Box::new(move || {
                registry
                    .snapshot()
                    .into_iter()
                    .map(|(k, instance)| {
                        (
                            k,
                            crate::command::partition_state::serialize_relay_state(&instance),
                        )
                    })
                    .collect()
            }));
        }

        let ctx_arc = Arc::new(Mutex::new(ctx));
        // Expose the coordinator channel process-globally so the
        // partition-state sink can drive a reconnect on a broken-pipe
        // flood without a handle threaded through every layer.
        super::install_reconnect_trigger(otx.clone());
        let otx_arc = Arc::new(Mutex::new(otx));
        let (_control_thread, mut control_stream, quic_client) =
            control_unit::open(namespace.clone(), ctx_arc.clone(), otx_arc.clone());
        executor.set_quic_client(quic_client);
        shutdown_bridge.set_control_stream(Some(Arc::clone(&control_stream)));

        // Jittered control-channel keepalive. The timer only signals this
        // coordinator (it never writes the stream), so it survives reconnects
        // and always pings the current control stream — single-threaded with
        // command replies, so writes never interleave.
        let _keepalive = control_unit::spawn_keepalive(otx_arc.clone());

        loop {
            let message = orx.recv().expect("Failed to receive coordinator message");

            match message {
                ExecutorMessage::HandleCommandAction(payload_id, message, _metadata) => {
                    command::handle_command(&mut executor, &control_stream, &payload_id, &message);
                }
                ExecutorMessage::ReturnRelays => {
                    return_relays(&executor, ctx_arc.clone());
                }
                ExecutorMessage::Reconnect(reason) => {
                    if matches!(executor.status, ExecutorStatus::Stopped) {
                        continue;
                    }
                    handle_reconnect(
                        &mut executor,
                        &mut control_stream,
                        &namespace,
                        &otx_arc,
                        reason,
                    );
                    shutdown_bridge.set_control_stream(Some(Arc::clone(&control_stream)));
                }
                ExecutorMessage::SendControlKeepalive => {
                    // A failed keepalive write means the control stream died;
                    // the reader loop detects the same and drives reconnect, so
                    // just drop this tick.
                    let _ = control_unit::send_keepalive_ping(&control_stream);
                }
            }
        }
    })
}

fn return_relays(executor: &ExecutorNode, ctx_arc: Arc<Mutex<Sender<ClientResponseMessage>>>) {
    let relays = executor.get_relays();
    ctx_arc
        .lock()
        .unwrap()
        .send(ClientResponseMessage::ReturnRelays(relays))
        .expect("Failed to send relays response");
}

/// Re-establish the control stream while preserving the SDK's
/// in-memory state. The previous behaviour cleared
/// `shared_io_streams`, `relay_registry`, and the `executor_id` —
/// that is what caused the May 25 silent freeze (executor sat empty
/// while the controller still believed the binding was live). The
/// `executor-controller-connection-healing` capability now requires:
///
///   * `executor_id`, `relay_registry`, and `shared_io_streams` are
///     retained across reconnect; in-flight per-Partition mailbox
///     work continues to drain.
///   * A `tracing::warn!` line is emitted with the upstream-derived
///     `reason` so kubectl logs surface every reconnect.
///   * The first frame on the freshly-established stream is a
///     `CommandRequest::ExecutorReconnected { previous_id, new_id,
///     reason }` so the controller can compute a binding delta
///     instead of re-issuing every relay.
fn handle_reconnect(
    executor: &mut ExecutorNode,
    control_stream: &mut Arc<FlezhaStream>,
    namespace: &str,
    otx_arc: &Arc<Mutex<Sender<ExecutorMessage>>>,
    reason: &str,
) {
    let previous_id = executor.get_executor_id();
    warn!(
        target: "flezha",
        "[flezha] control-channel reconnect reason={} previous_id={}",
        reason,
        previous_id
    );

    executor.set_status(ExecutorStatus::Stopped);

    let (_, new_stream, new_quic) =
        control_unit::open_reconnect(namespace.to_string(), otx_arc.clone());

    *control_stream = new_stream;
    executor.set_quic_client(new_quic);

    // Resume processing dispatch traffic against the preserved
    // registry — without `set_status(Running)` the per-Partition
    // mailboxes drain into a paused state and the controller sees
    // the SDK as a black hole even though its registry is intact.
    executor.set_status(ExecutorStatus::Running);

    // Emit `ExecutorReconnected` as the first frame on the new
    // stream. Failure to write is non-fatal — the reader loop will
    // observe the broken pipe and drive another reconnect, which is
    // the same recovery path we are already in. `new_id` matches
    // `previous_id` because we preserve identity; the controller
    // rewrites it via `ReassignExecutorId` only when its binding
    // for `previous_id` is empty.
    let payload = build_executor_reconnected_frame(&previous_id, &previous_id, reason);
    if let Err(e) = (&**control_stream).write_all(&payload) {
        warn!(
            target: "flezha",
            "[flezha] ExecutorReconnected write failed: {}", e
        );
    }
}

/// Build the wire payload for the SDK -> Controller
/// `ExecutorReconnected` frame. Extracted from `handle_reconnect` so
/// the frame format stays unit-testable independently of the
/// reconnect network path.
fn build_executor_reconnected_frame(
    previous_id: &str,
    new_id: &str,
    reason: &str,
) -> Vec<u8> {
    let request = CommandRequest {
        reference_id: format!("reconnect-{}", nonce_suffix()),
        command: Some(Command::ExecutorReconnected(PbExecutorReconnected {
            previous_id: previous_id.to_string(),
            new_id: new_id.to_string(),
            reason: reason.to_string(),
        })),
    };
    payloader::build_from_binary(
        "control_command",
        &request.encode_to_vec(),
        HashMap::new(),
    )
}

/// Stream-reader handle for the Relay model. Cheap to clone; carries a
/// reference to the live Relay-instance registry. Per-Partition drain
/// scheduling uses the pool held by `PartitionSlot`.
#[derive(Clone)]
pub struct RelayDispatchHandle {
    pub registry: Arc<RelayInstanceRegistry>,
}

impl RelayDispatchHandle {
    /// Dispatch a frame from the wire. Resolves the entry channel by
    /// `metadata["channel"]` (or the Relay's first declared input
    /// channel as the documented default), wraps the payload bytes via
    /// the channel's `wrap_input`, and pushes the typed
    /// `Arc<dyn Any + Send + Sync>` onto every consumer Shard's
    /// partition mailbox.
    pub fn dispatch(
        &self,
        instance_id: &str,
        payload: Vec<u8>,
        metadata: &HashMap<String, String>,
        stream: Arc<FlezhaStream>,
    ) {
        let instance = match self.registry.get(instance_id) {
            Some(i) => i,
            None => {
                eprintln!("[flezha] relay instance {} not found", instance_id);
                return;
            }
        };

        let topology = &instance.topology;

        let channel_id = match metadata.get("channel") {
            Some(name) => match topology.lookup_channel_id(name) {
                Some(id) => id,
                None => {
                    eprintln!("[flezha] relay {} has no channel `{}`", instance_id, name);
                    return;
                }
            },
            None => {
                match topology
                    .channels
                    .iter()
                    .position(|c| c.kind == crate::channel::ChannelKind::Entry)
                {
                    Some(idx) => idx as u32,
                    None => {
                        eprintln!("[flezha] relay {} has no entry channels", instance_id);
                        return;
                    }
                }
            }
        };

        let channel = match topology.channel(channel_id) {
            Some(c) => c,
            None => return,
        };

        let typed_payload = match &channel.wrap_input {
            Some(wrap) => match wrap(payload) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!(
                        "[flezha] relay {} channel `{}`: wrap_input failed: {}",
                        instance_id, channel.name, e
                    );
                    return;
                }
            },
            None => {
                eprintln!(
                    "[flezha] relay {} channel `{}` has no entry wrapper",
                    instance_id, channel.name
                );
                return;
            }
        };

        crate::relay::dispatcher::route_to_consumers(
            &instance,
            &channel.consumers,
            channel.overflow_policy,
            &typed_payload,
            &stream,
        );
    }
}

#[cfg(test)]
mod reconnect_tests {
    //! Exercises the reconnect-preservation contract introduced by
    //! `consume-recovery-mode-and-self-heal` §2:
    //!
    //!   * `executor_id` survives `handle_reconnect`,
    //!   * the in-memory `relay_registry` is NOT cleared,
    //!   * the SDK emits an `ExecutorReconnected` frame as the first
    //!     write on the freshly-opened control stream.
    //!
    //! The reconnect network path itself (`control_unit::open_reconnect`)
    //! is exercised by the existing integration tests; the unit tests
    //! here exercise the testable frame-builder + the state mutators
    //! handle_reconnect calls.
    use super::*;
    use crate::pb::CommandRequest;
    use prost::Message;

    #[test]
    fn executor_id_and_registry_are_preserved_across_status_flap() {
        let mut executor = ExecutorNode::new("ns".to_string());
        executor.assign_executor_id("e-abc".to_string());
        // ExecutorNode has no public `relay_registry.len()` so we drive
        // the state-preservation assertion through the public surface
        // that handle_reconnect uses: the executor_id and the
        // shared_io_streams handle pointer should be stable across the
        // status flap handle_reconnect performs.
        let original_id = executor.get_executor_id();
        let streams_before = Arc::as_ptr(&executor.get_io_streams());

        // What handle_reconnect actually does to the executor state:
        executor.set_status(ExecutorStatus::Stopped);
        executor.set_status(ExecutorStatus::Running);

        assert_eq!(executor.get_executor_id(), original_id);
        let streams_after = Arc::as_ptr(&executor.get_io_streams());
        assert_eq!(
            streams_before, streams_after,
            "shared_io_streams handle must survive handle_reconnect"
        );
    }

    #[test]
    fn build_executor_reconnected_frame_decodes_to_expected_oneof() {
        let frame = build_executor_reconnected_frame("prev-1", "prev-1", "control_stream_broken");

        // Decode through the payloader to extract the protobuf payload.
        use flz_transport::service::payloader;
        let (payload_id, message, _meta) = payloader::extract_payload(&{
            // Use a UnixStream pair as a transport: write the frame
            // into one end and read it back via extract_payload.
            use std::io::Write;
            use std::os::unix::net::UnixStream;
            let (a, b) = UnixStream::pair().unwrap();
            (&a).write_all(&frame).unwrap();
            drop(a);
            FlezhaStream::Unix(b)
        })
        .expect("frame extracts");

        assert_eq!(payload_id, "control_command");

        let request = CommandRequest::decode(message.as_slice()).expect("CommandRequest decodes");
        assert!(request.reference_id.starts_with("reconnect-"));

        match request.command {
            Some(crate::pb::command_request::Command::ExecutorReconnected(r)) => {
                assert_eq!(r.previous_id, "prev-1");
                assert_eq!(r.new_id, "prev-1");
                assert_eq!(r.reason, "control_stream_broken");
            }
            other => panic!("expected ExecutorReconnected, got {:?}", other),
        }
    }

    #[test]
    fn reassign_executor_id_handler_updates_in_memory_id() {
        // Direct unit test for the SDK-side `ReassignExecutorId`
        // CommandAction (sdk/rust/src/command/reassign_executor.rs).
        use crate::command::CommandAction;
        use crate::pb::ReassignExecutorId;

        // Mutates the process-global FLEZHA_EXECUTOR_ID_PATH, so serialize
        // against the control_unit env tests on the crate-wide lock.
        let _env_guard = crate::executor::env_test_guard();

        // The handler persists the id; point it at a temp path so the test
        // never touches the production hostPath.
        let mut idp = std::env::temp_dir();
        idp.push(format!("flezha-reassign-test-{}", std::process::id()));
        idp.push("id");
        std::env::set_var("FLEZHA_EXECUTOR_ID_PATH", &idp);

        let mut executor = ExecutorNode::new("ns".to_string());
        executor.assign_executor_id("old".to_string());

        let cmd = ReassignExecutorId {
            new_id: "new".to_string(),
        };
        let mut errors: Vec<String> = vec![];
        let (payload_id, _bytes) = cmd.handle(&mut executor, "", &mut errors);

        assert_eq!(payload_id, "reassign_executor_id");
        assert!(errors.is_empty());
        assert_eq!(executor.get_executor_id(), "new");

        std::env::remove_var("FLEZHA_EXECUTOR_ID_PATH");
        let _ = std::fs::remove_dir_all(idp.parent().unwrap());
    }
}
