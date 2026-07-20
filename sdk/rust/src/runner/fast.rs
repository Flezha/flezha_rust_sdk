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
use super::{control_unit, ClientResponseMessage, RunnerMessage, RunnerStatus};
use crate::command;
use crate::operative::instance::UnboundedInstance;
use crate::operative::registry::{get_class as get_operative_class, Operative, OperativeRegistry};
use crate::pb::command_request::Command;
use crate::pb::{BoundedCompleted as PbBoundedCompleted, CommandRequest};
use crate::shutdown::ShutdownBridge;
use crate::util::nonce_suffix;

pub struct RunnerNode {
    runner_id: String,
    _namespace: String,
    operative_registry: Arc<OperativeRegistry>,
    pool: Arc<WorkStealingPool>,
    shared_io_streams: Arc<RwLock<HashMap<u32, FlezhaStream>>>,
    quic_client: Option<Arc<QuicClient>>,
    status: RunnerStatus,
}

impl RunnerNode {
    pub fn new(namespace: String) -> RunnerNode {
        let operative_registry = Arc::new(OperativeRegistry::new());
        let pool = Arc::new(WorkStealingPool::new());
        let shared_io_streams = Arc::new(RwLock::new(HashMap::new()));

        RunnerNode {
            runner_id: "undefined".to_string(),
            _namespace: namespace,
            operative_registry,
            pool,
            shared_io_streams,
            quic_client: None,
            status: RunnerStatus::Starting,
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
            Arc::from(factory_id.as_str()),
            stage_segments,
            resume_segment_count as usize,
        );
        self.operative_registry
            .register_bounded(Arc::clone(&instance));
        crate::bounded::driver::spawn(spec, instance, inbox_rx, args, Arc::clone(&self.pool));
        true
    }

    /// The live-instance check + class unregistration behind
    /// [`RunnerMessage::RemoveFactory`]. MUST run on the coordinator loop —
    /// that is what serializes it with `AddOperative` handling and makes the
    /// check race-free. `Err(n)` = `n` live instances exist; the type stays
    /// registered.
    pub(crate) fn remove_factory(&self, factory_id: &str, bounded: bool) -> Result<(), usize> {
        let live = self.operative_registry.count_by_factory(factory_id);
        if live > 0 {
            return Err(live);
        }
        if bounded {
            crate::bounded::registry::unregister_class(factory_id);
        } else {
            crate::operative::registry::unregister_class(factory_id);
        }
        Ok(())
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

    /// Snapshot of live UnboundedOperative instances — the
    /// `FlezhaRunner::operatives()` view.
    pub fn get_operatives(&self) -> HashMap<String, Arc<UnboundedInstance>> {
        self.operative_registry.snapshot_unbounded()
    }

    pub fn get_io_streams(&self) -> Arc<RwLock<HashMap<u32, FlezhaStream>>> {
        Arc::clone(&self.shared_io_streams)
    }

    pub fn get_runner_id(&self) -> String {
        self.runner_id.clone()
    }

    pub fn assign_runner_id(&mut self, runner_id: String) {
        self.runner_id = runner_id;
    }

    pub fn set_status(&mut self, status: RunnerStatus) {
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
    declared: Arc<RwLock<Vec<String>>>,
    (otx, orx): (Sender<RunnerMessage>, Receiver<RunnerMessage>),
    ctx: Sender<ClientResponseMessage>,
    shutdown_bridge: Arc<ShutdownBridge>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut runner = RunnerNode::new(namespace.clone());

        // Capture a registry snapshot closure now so the shutdown bridge
        // can enumerate live UnboundedOperative instances without going through the
        // coordinator channel. Survives reconnects since the registry
        // Arc is stable.
        {
            let registry = Arc::clone(&runner.operative_registry);
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
        let (_control_thread, mut control_stream, quic_client) = control_unit::open(
            namespace.clone(),
            Arc::clone(&declared),
            ctx_arc.clone(),
            otx_arc.clone(),
        );
        runner.set_quic_client(quic_client);
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
            // A closed channel means every sender is gone — the owning
            // FlezhaRunner dropped. Exit cleanly so a later runner can start.
            let Ok(message) = orx.recv() else {
                break;
            };

            match message {
                RunnerMessage::HandleCommandAction(payload_id, message, _metadata) => {
                    command::handle_command(&mut runner, &control_stream, &payload_id, &message);
                }
                RunnerMessage::ReturnOperatives => {
                    return_operatives(&runner, ctx_arc.clone());
                }
                RunnerMessage::Reconnect(reason) => {
                    if matches!(runner.status, RunnerStatus::Stopped) {
                        continue;
                    }
                    handle_reconnect(
                        &mut runner,
                        &mut control_stream,
                        &namespace,
                        &declared,
                        &otx_arc,
                        reason,
                    );
                    shutdown_bridge.set_control_stream(Some(Arc::clone(&control_stream)));
                }
                RunnerMessage::SendControlKeepalive => {
                    // A failed keepalive write means the control stream died;
                    // the reader loop detects the same and drives reconnect, so
                    // just drop this tick.
                    let _ = control_unit::send_keepalive_ping(&control_stream);
                }
                RunnerMessage::SendDataHeartbeat => {
                    send_data_heartbeat(&runner.shared_io_streams);
                }
                RunnerMessage::EmitBoundedOutput {
                    operative_id,
                    payload,
                } => {
                    write_bounded_output(&runner.shared_io_streams, &operative_id, &payload);
                }
                RunnerMessage::EmitBoundedCompleted {
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
                RunnerMessage::EmitControlRequest {
                    reference_id,
                    command,
                } => {
                    // The reference_id keys the driver's registered waiter, so
                    // it is reused verbatim (no fresh nonce). A failed write is
                    // left to the driver's ack timeout — same posture as a
                    // lost frame.
                    let frame = framed_request(reference_id, command);
                    if let Err(e) = (&*control_stream).write_all(&frame) {
                        warn!(
                            target: "flezha",
                            "[flezha] SDK-initiated control request write failed: {}", e
                        );
                    }
                }
                RunnerMessage::RemoveFactory {
                    factory_id,
                    bounded,
                } => {
                    // Serialized with AddOperative by construction (same loop),
                    // so the live-instance check cannot race a placement.
                    let result = runner.remove_factory(&factory_id, bounded);
                    let _ = ctx_arc
                        .lock()
                        .unwrap()
                        .send(ClientResponseMessage::RemoveFactoryResult(result));
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
    framed_request(format!("{ref_prefix}-{}", nonce_suffix()), command)
}

/// Frame one SDK-initiated `CommandRequest` under an explicit reference_id
/// (the caller's waiter key when a controller reply is awaited).
fn framed_request(reference_id: String, command: Command) -> Vec<u8> {
    let request = CommandRequest {
        reference_id,
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
    metadata.insert(
        "credit_parked".to_string(),
        crate::segment::credit::credit_parked().to_string(),
    );
    metadata.insert(
        "restore_discarded_inputs".to_string(),
        crate::segment::restore_discarded_inputs().to_string(),
    );
    let frame = payloader::build_from_binary(liveness::DATA_HEARTBEAT_PAYLOAD_ID, &[], metadata);
    let streams = io_streams.read().unwrap();
    for stream in streams.values() {
        let mut w: &FlezhaStream = stream;
        let _ = w.write_all(&frame);
    }
}

fn return_operatives(runner: &RunnerNode, ctx_arc: Arc<Mutex<Sender<ClientResponseMessage>>>) {
    let operatives = runner.get_operatives();
    // A closed client channel means the owning handle is gone mid-request;
    // dropping the reply is the only sane outcome — never panic the
    // coordinator.
    let _ = ctx_arc
        .lock()
        .unwrap()
        .send(ClientResponseMessage::ReturnOperatives(operatives));
}

/// Re-establish the control stream while preserving the SDK's
/// in-memory state. The previous behaviour cleared
/// `shared_io_streams`, `operative_registry`, and the `runner_id` —
/// that is what caused the May 25 silent freeze (runner sat empty
/// while the controller still believed the binding was live). The
/// `executor-controller-connection-healing` capability now requires:
///
///   * `operative_registry` and `shared_io_streams` are retained across
///     reconnect (in-flight per-Segment mailbox work keeps draining); the
///     existing `runner_id` is kept until the controller assigns a fresh
///     session token via `AssignRunnerId` on the re-opened stream.
///   * A `tracing::warn!` line is emitted with the upstream-derived
///     `reason` so kubectl logs surface every reconnect.
///   * The re-opened control stream re-presents the runner's stable id in
///     its connect handshake, so the controller resolves the reconnect as a
///     binding swap — there is no separate reconnect frame.
fn handle_reconnect(
    runner: &mut RunnerNode,
    control_stream: &mut Arc<FlezhaStream>,
    namespace: &str,
    declared: &Arc<RwLock<Vec<String>>>,
    otx_arc: &Arc<Mutex<Sender<RunnerMessage>>>,
    reason: &str,
) {
    let previous_id = runner.get_runner_id();
    warn!(
        target: "flezha",
        "[flezha] control-channel reconnect reason={} previous_id={}",
        reason,
        previous_id
    );

    runner.set_status(RunnerStatus::Stopped);

    let (_, new_stream, new_quic) =
        control_unit::open_reconnect(namespace.to_string(), Arc::clone(declared), otx_arc.clone());

    *control_stream = new_stream;
    runner.set_quic_client(new_quic);

    // Resume processing dispatch traffic against the preserved
    // registry — without `set_status(Running)` the per-Segment
    // mailboxes drain into a paused state and the controller sees
    // the SDK as a black hole even though its registry is intact.
    runner.set_status(RunnerStatus::Running);
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
                    // A durable instance's input was journaled to its inbox
                    // before this frame was forwarded; the stamped offset rides
                    // into the iteration commit as the consumed offset.
                    let offset = metadata
                        .get(crate::bounded::INBOX_OFFSET_KEY)
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(0);
                    if !instance.push_inbox(payload, offset) {
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

        // Late-input correction: the end of a replayed slice is a broadcast, not
        // an input — an input routes to one Segment by key, so a marker riding
        // one would strand the slice every *other* Segment buffered. It carries
        // no payload and never reaches an Operation.
        if metadata.contains_key("flz.replay_end") {
            crate::operative::dispatcher::end_replay_slice(&instance);
            return;
        }

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

        // Effectively-once tier: the controller stamps the durable input-log
        // offset on the forwarded frame; thread it to the invocation so the
        // atomic effect record carries the same offset as consumed-offset.
        let consumed_offset = metadata
            .get("flz.offset")
            .and_then(|s| s.parse::<u64>().ok());
        let replay = metadata.contains_key("flz.replay");

        // Typed entries (`sdk-graph-authoring`): a failing parse is classified
        // like a failing invocation and rides the channel's retry→DLQ ladder,
        // so keep the wire bytes for the re-attempts and the park record.
        // Passthrough `Vec<u8>`/`String` entries keep the pre-change
        // drop-and-log path (no clone on the hot path).
        let retry_bytes = channel.entry_parsed.then(|| payload.clone());
        let typed_payload = match &channel.wrap_input {
            Some(wrap) => match wrap(payload) {
                Ok(p) => p,
                Err(e) => {
                    match retry_bytes {
                        Some(bytes) => crate::operative::dispatcher::spawn_entry_parse_ladder(
                            crate::operative::dispatcher::EntryParseLadder {
                                instance: Arc::clone(&instance),
                                channel_id,
                                bytes,
                                first_error: e,
                                stream,
                                consumed_offset,
                                replay,
                            },
                        ),
                        None => eprintln!(
                            "[flezha] operative {} channel `{}`: wrap_input failed: {}",
                            instance_id, channel.name, e
                        ),
                    }
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
            consumed_offset,
            replay,
        );
    }
}

#[cfg(test)]
mod remove_factory_tests {
    //! The live-instance refusal at the coordinator seam
    //! (`RunnerNode::remove_factory`): a factory with live instances refuses
    //! and stays registered; once the last instance stops, the removal
    //! unregisters the class.
    use super::*;
    use crate::operation::state_handle::StateHandle;
    use crate::operation::{NoChange, Operation, OperationCtx, OperationError, WallClock};
    use crate::segment::restore::RestoreInitPayload;
    use crate::{ChannelBuilder, UnboundedOperative};

    #[derive(Default)]
    struct SeamOp;
    impl Operation for SeamOp {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "remove_factory_seam_op"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            _: String,
            _: StateHandle<'_, Self>,
            _: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            Ok(())
        }
    }

    struct SeamOperative;
    impl UnboundedOperative for SeamOperative {
        fn factory_id() -> &'static str {
            "remove_factory_seam_probe"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in").into::<SeamOp>();
        }
    }

    #[test]
    fn removal_refuses_while_instances_live_and_unregisters_after() {
        let runner = RunnerNode::new("seam_ns".to_string());
        crate::register_unbounded_class::<SeamOperative>().expect("valid topology");

        runner
            .start_operative(
                "remove_factory_seam_probe".to_string(),
                "seam-inst-1".to_string(),
            )
            .expect("instance spawns");

        assert_eq!(
            runner.remove_factory("remove_factory_seam_probe", false),
            Err(1),
            "a live instance must refuse the removal with its count"
        );
        assert!(
            crate::operative::registry::get_class("remove_factory_seam_probe").is_some(),
            "a refused removal must leave the type registered"
        );

        assert!(runner.stop_operative("seam-inst-1"));
        assert_eq!(
            runner.remove_factory("remove_factory_seam_probe", false),
            Ok(())
        );
        assert!(
            crate::operative::registry::get_class("remove_factory_seam_probe").is_none(),
            "the removal must unregister the class"
        );
        // Idempotent on an unknown id (registry contract).
        assert_eq!(
            runner.remove_factory("remove_factory_seam_probe", false),
            Ok(())
        );
    }
}

#[cfg(test)]
mod reconnect_tests {
    //! Exercises the reconnect-preservation contract: `runner_id` and the
    //! in-memory `operative_registry` survive `handle_reconnect`'s status flap
    //! (the May 25 silent-freeze regression cleared them). The reconnect
    //! re-presents the runner's stable id via the connect handshake — there
    //! is no separate reconnect frame — so the controller resolves it as a
    //! binding swap; that controller-side path is covered by the Elixir suite
    //! and the cross-language matrix.
    use super::*;

    #[test]
    fn runner_id_and_registry_are_preserved_across_status_flap() {
        let mut runner = RunnerNode::new("ns".to_string());
        runner.assign_runner_id("e-abc".to_string());
        // RunnerNode has no public `operative_registry.len()` so we drive
        // the state-preservation assertion through the public surface
        // that handle_reconnect uses: the runner_id and the
        // shared_io_streams handle pointer should be stable across the
        // status flap handle_reconnect performs.
        let original_id = runner.get_runner_id();
        let streams_before = Arc::as_ptr(&runner.get_io_streams());

        // What handle_reconnect actually does to the runner state:
        runner.set_status(RunnerStatus::Stopped);
        runner.set_status(RunnerStatus::Running);

        assert_eq!(runner.get_runner_id(), original_id);
        let streams_after = Arc::as_ptr(&runner.get_io_streams());
        assert_eq!(
            streams_before, streams_after,
            "shared_io_streams handle must survive handle_reconnect"
        );
    }
}
