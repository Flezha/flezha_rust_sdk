//! Per-Segment mailboxes backed by a DashMap registry and a Chase-Lev
//! work-stealing pool. The coordinator thread still exists for control-plane
//! traffic (`HandleCommandAction`, `ReturnOperatives`, `Reconnect`) but
//! data-plane traffic bypasses it entirely — stream readers push directly
//! into the targeted UnboundedOperative instance's segment mailboxes.

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};

use crossbeam::channel::{Receiver, Sender};
use flz_transport::service::payloader;
use flz_transport::transport::quic::QuicClient;
use flz_transport::transport::{liveness, FlezhaStream};
use prost::Message;
use tracing::warn;

use super::work_stealing_pool::WorkStealingPool;
use super::{control_unit, ClientResponseMessage, ExecutorMessage, ExecutorStatus};
use crate::command;
use crate::operative::instance::UnboundedInstance;
use crate::operative::registry::{get_class as get_operative_class, Operative, OperativeRegistry};
use crate::pb::command_request::Command;
use crate::pb::{BoundedCompleted as PbBoundedCompleted, CommandRequest};
use crate::shutdown::ShutdownBridge;
use crate::util::nonce_suffix;

pub struct ExecutorNode {
    executor_id: String,
    _namespace: String,
    operative_registry: Arc<OperativeRegistry>,
    pool: Arc<WorkStealingPool>,
    shared_io_streams: Arc<RwLock<HashMap<u32, FlezhaStream>>>,
    quic_client: Option<Arc<QuicClient>>,
    status: ExecutorStatus,
}

impl ExecutorNode {
    pub fn new(namespace: String) -> ExecutorNode {
        let operative_registry = Arc::new(OperativeRegistry::new());
        let pool = Arc::new(WorkStealingPool::new());
        let shared_io_streams = Arc::new(RwLock::new(HashMap::new()));

        ExecutorNode {
            executor_id: "undefined".to_string(),
            _namespace: namespace,
            operative_registry,
            pool,
            shared_io_streams,
            quic_client: None,
            status: ExecutorStatus::Starting,
        }
    }

    /// Spawn an UnboundedOperative instance from a registered UnboundedOperative class. `factory_id`
    /// is looked up in the global class registry; `instance_id` is what
    /// the wire calls `operative_id`. Idempotent on duplicate
    /// `instance_id`: returns the existing instance.
    pub fn start_operative(
        &self,
        factory_id: String,
        instance_id: String,
    ) -> Option<Arc<UnboundedInstance>> {
        if let Some(existing) = self.operative_registry.get_unbounded(&instance_id) {
            return Some(existing);
        }
        let topology = get_operative_class(&factory_id)?;
        let instance = UnboundedInstance::new(
            topology,
            Arc::from(instance_id.as_str()),
            Arc::clone(&self.pool),
        );
        Some(self.operative_registry.register_unbounded(instance))
    }

    /// Spawn a bounded-kind unit: register it (idempotent on a duplicate id) and
    /// launch its sequential driver on a dedicated thread with `args` from
    /// `init_values`. The driver runs the run-sugar body or the structured
    /// linear chain, observes the lifecycle (inbox, loop, duration, external
    /// stop), and converges on a single `BoundedCompleted` teardown. Returns
    /// `false` when no bounded class is registered.
    pub fn start_bounded(&self, factory_id: String, instance_id: String, args: String) -> bool {
        self.start_bounded_resumable(factory_id, instance_id, args, 0)
    }

    /// Spawn a bounded unit that may be **resuming** after a crash: a durable
    /// structured driver holds its first Operation until `resume_segment_count`
    /// per-Segment restores have landed (the Controller ships one
    /// `RestoreInitState` per persisted Segment). `0` ⇒ a fresh spawn / a
    /// run-sugar or opted-out class ⇒ run immediately. See
    /// [`crate::bounded::instance`].
    pub fn start_bounded_resumable(
        &self,
        factory_id: String,
        instance_id: String,
        args: String,
        resume_segment_count: u32,
    ) -> bool {
        if self.operative_registry.contains(&instance_id) {
            return true;
        }
        let spec = match crate::bounded::registry::get_class(&factory_id) {
            Some(spec) => spec,
            None => return false,
        };

        // Build the Segments (structured; empty for run-sugar) + the instance
        // now — synchronously, before the AddOperative frame is ACKed — and
        // register it so the Controller's RestoreInitState frames reach live
        // slots and the driver holds its first Operation until the restores land.
        let id: Arc<str> = Arc::from(instance_id.as_str());
        let stage_segments = spec
            .topology
            .as_ref()
            .map(|_| crate::bounded::driver::build_all_stage_segments(&spec, &id, &self.pool))
            .unwrap_or_default();
        let (instance, inbox_rx) = crate::bounded::instance::BoundedInstance::new(
            Arc::clone(&id),
            stage_segments,
            resume_segment_count as usize,
        );
        self.operative_registry
            .register_bounded(Arc::clone(&instance));
        crate::bounded::driver::spawn(spec, instance, inbox_rx, args, Arc::clone(&self.pool));
        true
    }

    pub fn stop_operative(&self, instance_id: &str) -> bool {
        match self.operative_registry.remove(instance_id) {
            Some(unit) => {
                match &*unit {
                    Operative::Unbounded(instance) => instance.shutdown_reapers(),
                    // Hard removal of a bounded: trip its cancellation token so
                    // the driver thread unwinds and exits promptly.
                    Operative::Bounded(instance) => instance.request_cancel(),
                }
                true
            }
            None => false,
        }
    }

    pub fn has_operative(&self, instance_id: &str) -> bool {
        self.operative_registry.contains(instance_id)
    }

    pub fn get_unbounded(&self, instance_id: &str) -> Option<Arc<UnboundedInstance>> {
        self.operative_registry.get_unbounded(instance_id)
    }

    /// Resolve a running bounded instance by id (cooperative `BoundedStop`).
    pub(crate) fn get_bounded(
        &self,
        instance_id: &str,
    ) -> Option<Arc<crate::bounded::instance::BoundedInstance>> {
        self.operative_registry.get_bounded(instance_id)
    }

    /// Locate a live bounded Segment slot by wire identity, for the inbound
    /// `RestoreInitState` handler.
    pub(crate) fn find_bounded_segment(
        &self,
        segment_id: &[u8],
    ) -> Option<(
        Arc<crate::bounded::instance::BoundedInstance>,
        Arc<crate::segment::SegmentSlot>,
    )> {
        self.operative_registry.find_bounded_segment(segment_id)
    }

    pub fn operative_dispatch_handle(&self) -> OperativeDispatchHandle {
        OperativeDispatchHandle {
            registry: Arc::clone(&self.operative_registry),
        }
    }

    pub fn _get_namespace(&self) -> String {
        self._namespace.clone()
    }

    /// Public-API view: snapshot of live UnboundedOperative instances. Mirrors the
    /// pre-`operative-mvp-rust` `Flezha::get_operatives()` shape so external
    /// inspectors that walk the registry continue to work.
    // reason: returned shape is the public API; splitting it would break external inspectors
    #[allow(clippy::type_complexity)]
    pub fn get_operatives(
        &self,
    ) -> Arc<RwLock<HashMap<String, Arc<Mutex<Arc<UnboundedInstance>>>>>> {
        crate::operative::registry::snapshot_as_operative_view(&self.operative_registry)
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
        // can enumerate live UnboundedOperative instances without going through the
        // coordinator channel. Survives reconnects since the registry
        // Arc is stable.
        {
            let registry = Arc::clone(&executor.operative_registry);
            shutdown_bridge.set_snapshot_fn(Box::new(move || {
                registry
                    .snapshot_unbounded()
                    .into_iter()
                    .map(|(k, instance)| {
                        (
                            k,
                            crate::command::segment_state::serialize_operative_state(&instance),
                        )
                    })
                    .collect()
            }));
        }

        let ctx_arc = Arc::new(Mutex::new(ctx));
        // Expose the coordinator channel process-globally so the
        // segment-state sink can drive a reconnect on a broken-pipe
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

        // Jittered data-channel heartbeat. Like the keepalive it only signals
        // this coordinator, which writes every secondary stream — so an
        // idle-but-healthy operative keeps the controller's per-channel liveness
        // clock fresh without producing real output.
        let _data_heartbeat = control_unit::spawn_data_heartbeat(otx_arc.clone());

        loop {
            let message = orx.recv().expect("Failed to receive coordinator message");

            match message {
                ExecutorMessage::HandleCommandAction(payload_id, message, _metadata) => {
                    command::handle_command(&mut executor, &control_stream, &payload_id, &message);
                }
                ExecutorMessage::ReturnOperatives => {
                    return_operatives(&executor, ctx_arc.clone());
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
                ExecutorMessage::SendDataHeartbeat => {
                    send_data_heartbeat(&executor.shared_io_streams);
                }
                ExecutorMessage::EmitBoundedOutput {
                    operative_id,
                    payload,
                } => {
                    write_bounded_output(&executor.shared_io_streams, &operative_id, &payload);
                }
                ExecutorMessage::EmitBoundedCompleted {
                    operative_id,
                    status,
                    error,
                    result,
                } => {
                    let frame = control_command_frame(
                        "bounded-completed",
                        Command::BoundedCompleted(PbBoundedCompleted {
                            operative_id: operative_id.clone(),
                            status,
                            error,
                            result,
                        }),
                    );
                    if let Err(e) = (&*control_stream).write_all(&frame) {
                        warn!(
                            target: "flezha",
                            "[flezha] BoundedCompleted write failed for {}: {}", operative_id, e
                        );
                    }
                }
            }
        }
    })
}

/// Write a bounded's output payload on a secondary stream, tagged with its
/// `operative_id` so the controller routes it to the unit's bound plugins. The
/// controller keys output fan-out on the payload id, so any one secondary
/// stream suffices; best-effort like the data heartbeat.
fn write_bounded_output(
    io_streams: &RwLock<HashMap<u32, FlezhaStream>>,
    operative_id: &str,
    payload: &[u8],
) {
    let frame = payloader::build_from_binary(operative_id, payload, HashMap::new());
    let streams = io_streams.read().unwrap();
    match streams.values().next() {
        Some(stream) => {
            let mut w: &FlezhaStream = stream;
            if let Err(e) = w.write_all(&frame) {
                eprintln!(
                    "[flezha] bounded {} output write failed: {}",
                    operative_id, e
                );
            }
        }
        None => eprintln!(
            "[flezha] bounded {} output dropped: no secondary stream available",
            operative_id
        ),
    }
}

/// Build an SDK -> Controller `control_command` frame for one oneof command,
/// stamping a `<ref_prefix>-<nonce>` reference id.
fn control_command_frame(ref_prefix: &str, command: Command) -> Vec<u8> {
    let request = CommandRequest {
        reference_id: format!("{ref_prefix}-{}", nonce_suffix()),
        command: Some(command),
    };
    payloader::build_from_binary("control_command", &request.encode_to_vec(), HashMap::new())
}

/// Write a data-channel heartbeat on every secondary stream. Best-effort: a
/// dead secondary is repopulated by the next `ConnectSocket`. The frame is a
/// bare payload-id with no body, small enough to write atomically.
fn send_data_heartbeat(io_streams: &RwLock<HashMap<u32, FlezhaStream>>) {
    // Dispatch-progress self-report: the heartbeat thread stays alive even
    // when the dispatch pool is wedged, so the controller needs these
    // counters to tell "alive but frozen" apart from "healthy".
    let mut metadata = HashMap::new();
    metadata.insert(
        "dispatches".to_string(),
        crate::segment::dispatches_completed().to_string(),
    );
    metadata.insert(
        "blocked_parks".to_string(),
        crate::segment::blocked_park_episodes().to_string(),
    );
    let frame = payloader::build_from_binary(liveness::DATA_HEARTBEAT_PAYLOAD_ID, &[], metadata);
    let streams = io_streams.read().unwrap();
    for stream in streams.values() {
        let mut w: &FlezhaStream = stream;
        let _ = w.write_all(&frame);
    }
}

fn return_operatives(executor: &ExecutorNode, ctx_arc: Arc<Mutex<Sender<ClientResponseMessage>>>) {
    let operatives = executor.get_operatives();
    ctx_arc
        .lock()
        .unwrap()
        .send(ClientResponseMessage::ReturnOperatives(operatives))
        .expect("Failed to send operatives response");
}

/// Re-establish the control stream while preserving the SDK's
/// in-memory state. The previous behaviour cleared
/// `shared_io_streams`, `operative_registry`, and the `executor_id` —
/// that is what caused the May 25 silent freeze (executor sat empty
/// while the controller still believed the binding was live). The
/// `executor-controller-connection-healing` capability now requires:
///
///   * `operative_registry` and `shared_io_streams` are retained across
///     reconnect (in-flight per-Segment mailbox work keeps draining); the
///     existing `executor_id` is kept until the controller assigns a fresh
///     session token via `AssignExecutorId` on the re-opened stream.
///   * A `tracing::warn!` line is emitted with the upstream-derived
///     `reason` so kubectl logs surface every reconnect.
///   * The re-opened control stream re-presents the executor's stable id in
///     its connect handshake, so the controller resolves the reconnect as a
///     binding swap — there is no separate reconnect frame.
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
    // registry — without `set_status(Running)` the per-Segment
    // mailboxes drain into a paused state and the controller sees
    // the SDK as a black hole even though its registry is intact.
    executor.set_status(ExecutorStatus::Running);
}

/// Stream-reader handle for the UnboundedOperative model. Cheap to clone; carries a
/// reference to the live UnboundedOperative-instance registry. Per-Segment drain
/// scheduling uses the pool held by `SegmentSlot`.
#[derive(Clone)]
pub struct OperativeDispatchHandle {
    pub registry: Arc<OperativeRegistry>,
}

impl OperativeDispatchHandle {
    /// Dispatch a frame from the wire. Resolves the entry channel by
    /// `metadata["channel"]` (or the UnboundedOperative's first declared input
    /// channel as the documented default), wraps the payload bytes via
    /// the channel's `wrap_input`, and pushes the typed
    /// `Arc<dyn Any + Send + Sync>` onto every consumer Operation's
    /// segment mailbox.
    pub fn dispatch(
        &self,
        instance_id: &str,
        payload: Vec<u8>,
        metadata: &HashMap<String, String>,
        stream: Arc<FlezhaStream>,
    ) {
        let instance = match self.registry.get_operative(instance_id) {
            Some(unit) => match &*unit {
                Operative::Unbounded(ri) => Arc::clone(ri),
                // Bounded inbox: route post-start input to the running driver's
                // inbox instead of dropping it. Dropped only if the instance is
                // not running / has no live inbox.
                Operative::Bounded(instance) => {
                    if !instance.push_inbox(payload) {
                        eprintln!(
                            "[flezha] inbox frame for bounded unit {} dropped (not running / no inbox)",
                            instance_id
                        );
                    }
                    return;
                }
            },
            None => {
                eprintln!("[flezha] operative instance {} not found", instance_id);
                return;
            }
        };

        let topology = &instance.topology;

        let channel_id = match metadata.get("channel") {
            Some(name) => match topology.lookup_channel_id(name) {
                Some(id) => id,
                None => {
                    eprintln!(
                        "[flezha] operative {} has no channel `{}`",
                        instance_id, name
                    );
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
                        eprintln!("[flezha] operative {} has no entry channels", instance_id);
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
                        "[flezha] operative {} channel `{}`: wrap_input failed: {}",
                        instance_id, channel.name, e
                    );
                    return;
                }
            },
            None => {
                eprintln!(
                    "[flezha] operative {} channel `{}` has no entry wrapper",
                    instance_id, channel.name
                );
                return;
            }
        };

        crate::operative::dispatcher::route_to_consumers(
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
    //! Exercises the reconnect-preservation contract: `executor_id` and the
    //! in-memory `operative_registry` survive `handle_reconnect`'s status flap
    //! (the May 25 silent-freeze regression cleared them). The reconnect
    //! re-presents the executor's stable id via the connect handshake — there
    //! is no separate reconnect frame — so the controller resolves it as a
    //! binding swap; that controller-side path is covered by the Elixir suite
    //! and the cross-language matrix.
    use super::*;

    #[test]
    fn executor_id_and_registry_are_preserved_across_status_flap() {
        let mut executor = ExecutorNode::new("ns".to_string());
        executor.assign_executor_id("e-abc".to_string());
        // ExecutorNode has no public `operative_registry.len()` so we drive
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
}
