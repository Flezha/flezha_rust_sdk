//! Producer-first fluent graph builder (`sdk-graph-authoring`).
//!
//! `c.input::<T>("name").into::<A>().into::<B>().output("sink")` — `.into`
//! wires a consumer and returns that operation's **output stream handle**
//! ([`Stream`]), so a linear pipeline is one chained expression. Branching is
//! handle reuse (hold the handle in a variable and wire it twice) or the
//! equivalent [`Stream::branch`] sugar. Fan-in is two streams naming the same
//! consumer node.
//!
//! Compile-time enforcement:
//! - `.into::<S>()` requires `S: Operation<Input = T>` — a type-mismatched
//!   consumer fires at the call site.
//! - Channel payload types come from the producer's declared `Output`; there
//!   is no user-supplied type token for inter-operation channels.
//! - `output(...)` returns a finalized [`Channel`] with no further builder
//!   methods, so nothing can be wired after the plugin-bound terminal.
//! - Handles borrow the builder (`Stream<'b, T>` is `Copy`), so a handle
//!   cannot outlive the declaration: using one after the topology freezes at
//!   registration is a compile error, not a runtime failure.

use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;

use crate::channel::{Channel, ChannelKind, OperationId, OverflowPolicy};
use crate::operation::Operation;

/// Type-erased dispatcher factory for one Operation. Stored on
/// `BuilderState::operations` and consumed by `build_topology` to produce
/// per-segment slots in each `UnboundedInstance`.
pub(crate) struct OperationEntry {
    /// Node identity: the operation's `name()`, or the explicit node name
    /// given at wiring time (`into_named`). Feeds everything `name()` feeds —
    /// state directories, journal entries, the derived channel name,
    /// diagnostics.
    pub name: &'static str,
    /// The Rust type behind this node, for the unification rule: two unnamed
    /// mentions of one class are the same node; a named mention is its own.
    pub class_id: TypeId,
    /// Whether this node was created with an explicit node name.
    pub named: bool,
    pub segment_keys: Vec<&'static str>,
    pub segment_for_overridden: bool,
    pub operation_key_overridden: bool,
    /// `Operation::COMPENSATE_OVERRIDDEN` captured at registration time. The
    /// bounded driver consults the resolved descriptor flag to fire saga
    /// compensation only for operations that declared a `compensate` hook.
    pub compensate_overridden: bool,
    /// Channels this node consumes. Several = fan-in: the node receives the
    /// accumulated contributions of all its producers.
    pub input_channel_ids: Vec<u32>,
    pub output_channel_id: Option<u32>,
    /// Erased input-type marker: matches the consumer-edge's
    /// `consumer_input_type_id` for compile-time-erased correctness checks.
    pub input_type_id: std::any::TypeId,
    /// Erased output-type marker: matches the producer-edge's payload type.
    pub output_type_id: std::any::TypeId,

    /// `Operation::Clock::type_name()` captured at registration time, plus
    /// whether the Operation's `Change` is `NoChange`. `build_topology`
    /// rejects clock configurations the runtime cannot serve (a
    /// `LogicalClock` without persistent changes never advances).
    pub clock_type_name: &'static str,
    /// `Clock::IS_EVENT_TIME` — whether this operation's clock yields a time
    /// carried by the input rather than one read off the wall or the WAL.
    /// Late-input correction is only definable against such a clock.
    pub clock_is_event_time: bool,
    pub change_is_nochange: bool,

    /// `Operation::mailbox_capacity()` captured at registration time. The
    /// resolved value (after applying env-var precedence in `build_topology`)
    /// is the size of every Segment mailbox built for this Operation.
    pub mailbox_capacity: usize,

    /// `Operation::MAILBOX_CAPACITY_OVERRIDDEN` captured at registration time.
    /// Drives the precedence rule in `resolve_mailbox_capacity`.
    pub mailbox_capacity_overridden: bool,

    /// Build a per-segment slot. The closure captures the Operation type
    /// statically so the segment mailbox carries `Arc<S::Input>` items
    /// and `Operation::run` is called with the typed signature.
    pub build_segment: SegmentBuilder,

    /// Segment routing for inputs targeting this Operation. Returns the
    /// segment keys. Single-mode operations return an empty vec; the runtime
    /// then routes everything to segment index 0.
    pub segment_for_erased: SegmentForFn,

    /// `Operation::operation_key` captured statically and type-erased. `Some`
    /// only for dynamic-mode Operations (`OPERATION_KEY_OVERRIDDEN = true` with
    /// empty `operation_keys()`); the dispatch path calls it per input to
    /// derive the dynamic segment key.
    pub operation_key_erased: Option<OperationKeyFn>,

    /// Dynamic-mode knobs captured at registration (`Operation::segment_idle_ttl`,
    /// `max_segments`). Carried through to the `OperationDescriptor` for the
    /// reaper / registry; inert for non-dynamic Operations. `idle_ttl == None`
    /// means no eviction.
    pub idle_ttl: Option<std::time::Duration>,
    pub max_segments: usize,

    /// Serialize a Segment's typed state to JSON. Captures S statically
    /// so the closure downcasts the type-erased `Box<dyn Any + Send>`
    /// stored in `SegmentSlot::state` and produces a `serde_json::Value`
    /// via the `S::State: Serialize` bound on the trait.
    pub serialize_state: SerializeStateFn,

    /// Deserialize a per-Operation JSON value into a fresh State and wrap
    /// it as the typed-erased payload `SegmentSlot::state` expects
    /// (a `Box<SegmentState<S>>`). Captures S statically; constructs
    /// a fresh `S` via `Default` since the user's `Operation` is stateless.
    pub deserialize_state: DeserializeStateFn,
}

pub(crate) type SegmentForFn = Arc<dyn Fn(&dyn Any) -> Vec<&'static str> + Send + Sync + 'static>;

pub(crate) type OperationKeyFn = Arc<dyn Fn(&dyn Any) -> Option<String> + Send + Sync + 'static>;

pub(crate) type SerializeStateFn =
    Arc<dyn Fn(&Box<dyn Any + Send>) -> Result<serde_json::Value, String> + Send + Sync + 'static>;

pub(crate) type DeserializeStateFn =
    Arc<dyn Fn(&serde_json::Value) -> Result<Box<dyn Any + Send>, String> + Send + Sync + 'static>;

pub(crate) type SegmentBuilder =
    Box<dyn Fn(BuildSegmentArgs<'_>) -> Arc<crate::segment::SegmentSlot> + Send + Sync + 'static>;

pub struct BuildSegmentArgs<'a> {
    pub topology: Arc<crate::operative::topology::OperativeTopology>,
    pub instance: std::sync::Weak<crate::operative::instance::UnboundedInstance>,
    /// The operative INSTANCE id (controller-assigned operative_id). The on-disk WAL
    /// is keyed by this, not the factory id, so two operatives of the same factory
    /// keep independent per-Segment state instead of colliding on one WAL.
    pub instance_id: Arc<str>,
    pub operation_id: OperationId,
    pub operation_key: Option<&'a str>,
    /// Zero-based segment index within the Operation. `0` for single-mode
    /// operations; `0..operation_keys().len()` for static-mode operations.
    pub segment_index: usize,
    /// Frozen-at-instance-start persistence flag, propagated from the
    /// `UnboundedInstance`. The `SegmentSlot` carries this on its
    /// `persistence_enabled` field; the per-process flusher inspects it
    /// at drain time to decide whether to call `send_frame`.
    pub persistence_enabled: bool,
    pub pool: Arc<crate::runner::work_stealing_pool::WorkStealingPool>,
}

/// Plugin sink attached to an `InterOperation` channel via
/// `also_output_with(...)` / `also_output(...)`. The presence of this field
/// flips the channel from pure inter-operation fan-out to hybrid (consumers +
/// plugin wire) without changing `ChannelKind`.
pub(crate) struct PluginSink {
    pub name: &'static str,
    pub serializer: crate::channel::PluginSerializeFn,
}

/// Internal channel record used during topology construction.
pub(crate) struct ChannelEntry {
    pub name: &'static str,
    pub kind: ChannelKind,
    pub producer: Option<OperationId>,
    pub consumers: Vec<OperationId>,
    pub plugin_name: Option<&'static str>,
    pub wrap_input: Option<crate::channel::EntryWrapFn>,
    /// Entry channels whose `wrap_input` is a real parser (typed JSON or a
    /// custom `input_with` parser). A failing parse rides the channel's
    /// retry→DLQ ladder; `Vec<u8>`/`String` passthrough entries keep the
    /// pre-change drop-and-log behaviour on invalid input.
    pub entry_parsed: bool,
    pub plugin_serializer: Option<crate::channel::PluginSerializeFn>,
    /// Hybrid plugin sink: populated by `also_output_with(...)` on
    /// an `InterOperation` channel. Mutually exclusive with
    /// `plugin_name`/`plugin_serializer`, which are populated by
    /// `output(...)` on a pure `PluginBound` channel.
    pub plugin_sink: Option<PluginSink>,
    /// Output-identity opt-in set by `.keyed()`. Valid only on channels
    /// with a plugin wire; `build_topology` rejects a keyed channel that
    /// has neither `output` nor `also_output_with`.
    pub keyed: bool,
    /// Backpressure policy applied at the push site when the consumer
    /// mailbox is full. Initialized at channel-creation time per channel
    /// kind (Entry → DropNewest, InterOperation → Block); plugin-bound
    /// declarations carry a placeholder that is never consulted because
    /// `emit_plugin` writes straight to the wire. `.on_full(...)`
    /// overwrites this value.
    pub overflow_policy: OverflowPolicy,
    /// Retry-then-park policy for this channel's consumer dispatchers.
    /// `.retry_max(...)` / `.retry_backoff_ms(...)` overwrite the defaults.
    pub retry: crate::channel::RetryPolicy,
    /// Inter-op byte codec declared via `.codec()` / `.codec_with(...)`.
    /// Required for durable structured bounded classes (`bounded-op-journal`).
    pub codec: Option<crate::channel::ChannelCodec>,
}

/// Reasons `UnboundedOperative::channels(c)` declarations fail at registration time.
#[derive(Debug, thiserror::Error)]
pub enum TopologyIssue {
    #[error("channel `{0}` declared more than once on this operative")]
    DuplicateChannel(&'static str),

    #[error(
        "node identity `{name}` is declared by two distinct nodes; a node name must be unique \
         per operative and distinct from every operation name()"
    )]
    NodeIdentityCollision { name: &'static str },

    #[error("branch(...) requires at least two arms; {arms} given")]
    BranchTooFewArms { arms: usize },

    #[error(
        "operation `{operation}` is on a cycle; an operative topology is a DAG — a node's \
         output may never reach its own input"
    )]
    Cycle { operation: &'static str },

    #[error(
        "operative declares more than one entry channel (`{first}` and `{second}`); the graph \
         is fed by exactly one entry"
    )]
    MultipleEntryChannels {
        first: &'static str,
        second: &'static str,
    },

    #[error("operation `{operation}` is not reachable from the entry channel")]
    UnreachableOperation { operation: &'static str },

    #[error(
        "operation `{name}` declares non-empty operation_keys() but does not override segment_for \
         (set SEGMENT_FOR_OVERRIDDEN = true)"
    )]
    SegmentForMissing { name: &'static str },

    #[error(
        "operation `{name}` declares both operation_keys() (static mode) and overrides operation_key() \
         (dynamic mode); a Operation declares one OR the other, not both"
    )]
    StaticAndDynamic { name: &'static str },

    #[error("operation `{name}` registered with conflicting input/output types")]
    TypeMismatch { name: &'static str },

    #[error(
        "operation `{operation}` declared mailbox_capacity = 0; capacity must be in 1..=1_048_576"
    )]
    MailboxCapacityZero { operation: &'static str },

    #[error(
        "operation `{operation}` declared mailbox_capacity = {value}; capacity must be in 1..=1_048_576"
    )]
    MailboxCapacityTooLarge {
        operation: &'static str,
        value: usize,
    },

    #[error(
        "channel `{channel}` was declared with `also_output_with(...)` more than once; \
         only one plugin sink per channel is allowed"
    )]
    DuplicatePluginSink { channel: &'static str },

    #[error(
        "channel `{channel}` is declared .keyed() but has no plugin wire; output identity \
         stamps only exist on plugin-bound frames (add .output(...) or \
         .also_output_with(...))"
    )]
    KeyedChannelWithoutPluginWire { channel: &'static str },

    #[error(
        "operation `{operation}` declares clock `{clock_type}` with `type Change = NoChange`; \
         the runtime cannot serve it (logical time is the durable change seq, which never \
         advances without persistent changes)"
    )]
    UnservableClock {
        operation: &'static str,
        clock_type: &'static str,
    },

    #[error(
        "channel `{channel}` has a plugin wire but is not declared .keyed(); the \
         effectively-once tier deduplicates by output identity, so every plugin-bound \
         channel of an effectively_once() operative must opt in with .keyed()"
    )]
    UnkeyedChannelUnderEffectivelyOnce { channel: &'static str },

    #[error(
        "channel `{channel}` declared retry_max = 0; at least one attempt is required \
         (use OperationError::non_retryable to park without retries)"
    )]
    RetryMaxZero { channel: &'static str },

    #[error(
        "operative declares correction_window() without effectively_once(); correction \
         replays the input log against a rewound baseline, and only the tier journals \
         inputs before forwarding them (an at-least-once operative has no durable input \
         to replay)"
    )]
    CorrectionWithoutTier,

    #[error(
        "operative declares correction_window() but operation `{operation}` declares clock \
         `{clock_type}`; correction orders a replayed slice by event time, which only \
         `EventTimeFrom` yields"
    )]
    CorrectionWithoutEventTime {
        operation: &'static str,
        clock_type: &'static str,
    },

    #[error("operative declares correction_window(0); a zero window can correct nothing")]
    CorrectionWindowZero,

    #[error(
        "operative declares checkpoint_ack() without effectively_once(); the checkpoint is \
         where the tier's consumed-offset advances, and an at-least-once operative has no \
         durable offset to hold back (nor a durable input log to replay the interval from)"
    )]
    CheckpointAckWithoutTier,
}

/// Mutable topology under construction, behind the builder's `RefCell` so the
/// `Copy` stream handles can share it.
pub(crate) struct BuilderState {
    pub channels: Vec<ChannelEntry>,
    pub operations: Vec<OperationEntry>,
    pub operation_name_to_id: HashMap<&'static str, u32>,
    pub errors: Vec<TopologyIssue>,
    pub effectively_once: bool,
    pub correction: Option<crate::operative::topology::CorrectionPolicy>,
    pub ack_granularity: crate::operative::topology::AckGranularity,
}

/// Builder collected by `UnboundedOperative::channels`. Internal state turns into a
/// frozen `OperativeTopology` at `build_topology` time. Stream handles hold a
/// shared reference to the builder, so its interior is a `RefCell`.
pub struct ChannelBuilder {
    pub(crate) state: RefCell<BuilderState>,
}

impl ChannelBuilder {
    pub(crate) fn new() -> Self {
        Self {
            state: RefCell::new(BuilderState {
                channels: Vec::new(),
                operations: Vec::new(),
                operation_name_to_id: HashMap::new(),
                errors: Vec::new(),
                effectively_once: false,
                correction: None,
                ack_granularity: crate::operative::topology::AckGranularity::PerInput,
            }),
        }
    }

    /// Freeze: hand the collected state to `build_topology`. Consuming the
    /// builder here is what invalidates every outstanding stream handle — they
    /// borrow it, so a handle used after the freeze is a compile error.
    pub(crate) fn into_state(self) -> BuilderState {
        self.state.into_inner()
    }

    /// Whether any operation was declared (`false` selects the bounded
    /// run-sugar path).
    pub(crate) fn has_operations(&self) -> bool {
        !self.state.borrow().operations.is_empty()
    }

    /// Opt this operative into late-input correction over the given window.
    ///
    /// An input whose extracted event time is behind its Segment's high-water
    /// by no more than `window_ms` is not processed as an arrival. The engine
    /// rewinds the Segment to a baseline preceding that event time, replays
    /// the retained input slice in event-time order under a bumped epoch, and
    /// keyed changelog sinks overwrite the stale rows by natural key.
    ///
    /// Requires `effectively_once()` (correction replays the durable input
    /// log) and an `EventTimeFrom` clock (there is nothing else to order by).
    /// `build_topology` refuses the declaration otherwise. The controller
    /// additionally refuses to start the operative unless its input-log hold
    /// window and snapshot age-floor both cover `window_ms` — you cannot
    /// rebuild a past you did not retain.
    ///
    /// Inputs later than the window follow [`beyond_window_policy`], which
    /// defaults to processing them as arrivals.
    ///
    /// [`beyond_window_policy`]: ChannelBuilder::beyond_window_policy
    pub fn correction_window(&self, window_ms: u64) {
        let mut st = self.state.borrow_mut();
        let beyond_window = st.correction.map(|c| c.beyond_window).unwrap_or_default();
        st.correction = Some(crate::operative::topology::CorrectionPolicy {
            window_ms,
            beyond_window,
        });
    }

    /// Fate of an input later than the declared correction window. Defaults to
    /// [`BeyondWindowPolicy::ProcessAsArrival`] — the behaviour of an operative
    /// with no correction window at all, so declaring one never silently starts
    /// dropping data.
    ///
    /// [`BeyondWindowPolicy::ProcessAsArrival`]: crate::operative::topology::BeyondWindowPolicy::ProcessAsArrival
    pub fn beyond_window_policy(&self, policy: crate::operative::topology::BeyondWindowPolicy) {
        let mut st = self.state.borrow_mut();
        let window_ms = st.correction.map(|c| c.window_ms).unwrap_or(0);
        st.correction = Some(crate::operative::topology::CorrectionPolicy {
            window_ms,
            beyond_window: policy,
        });
    }

    /// Opt this operative into the effectively-once delivery tier.
    ///
    /// In tier mode each invocation's plugin emits ship inside the atomic
    /// effect record on the reliable state sink (with the state change and
    /// consumed input offset), and the controller flushes them to plugins
    /// only after the record is fsynced. Identity is the dedup contract:
    /// `build_topology` rejects the declaration when any plugin-bound
    /// channel lacks `.keyed()`. Default is off — operatives that do not
    /// call this keep the at-least-once paths byte-identical.
    pub fn effectively_once(&self) {
        self.state.borrow_mut().effectively_once = true;
    }

    /// Advance the durable consumed-offset only at an explicit
    /// [`OperationCtx::checkpoint`], instead of once per input.
    ///
    /// For an operative that folds many inputs into one state push — a window
    /// that only closes on its boundary — the per-input default is not merely
    /// expensive, it is incoherent: the offset would run ahead of the state,
    /// and a restore would resume past inputs the restored state never saw.
    /// Under this granularity the changes an interval applies are held until
    /// `ctx.checkpoint()` and ship with the closing input's offset as one
    /// fsynced record, so state and offset advance together or not at all.
    ///
    /// The price is the interval: on recovery the controller re-forwards
    /// every input after the last checkpoint, and the interval's outputs are
    /// emitted again. Both are absorbed by the keyed-sink dedup the tier
    /// already requires — and the interval you choose is also the input-log
    /// retention and the recovery work you are asking for.
    ///
    /// Requires `effectively_once()`; `build_topology` refuses it otherwise.
    /// Emits made before a checkpoint are still flushed as they happen —
    /// checkpointing moves the *acknowledgement*, not the output.
    ///
    /// [`OperationCtx::checkpoint`]: crate::operation::OperationCtx::checkpoint
    pub fn checkpoint_ack(&self) {
        self.state.borrow_mut().ack_granularity =
            crate::operative::topology::AckGranularity::Checkpoint;
    }

    /// Declare the entry channel fed by the controller — the operative's
    /// external input address, so its name is explicit and mandatory.
    ///
    /// Any payload type is accepted. `Vec<u8>` is delivered unchanged and
    /// `String` UTF-8-decoded (invalid UTF-8 is dropped and logged), exactly
    /// as before; any other `T` is deserialized at the entry edge from the
    /// wire bytes with the SDK's default JSON codec (`serde_json`). A failing
    /// parse is classified like a failing invocation and rides this channel's
    /// retry→DLQ ladder. For a non-JSON wire format, use
    /// [`input_with`](ChannelBuilder::input_with).
    pub fn input<T: DeserializeOwned + Send + Sync + 'static>(
        &self,
        name: &'static str,
    ) -> EntryStream<'_, T> {
        let (wrap, parsed) = default_entry_wrap::<T>();
        self.entry_channel(name, wrap, parsed)
    }

    /// Declare the entry channel with a custom `bytes → T` parser, replacing
    /// the default JSON codec of [`input`](ChannelBuilder::input).
    ///
    /// The parser MUST be deterministic — the same bytes always produce the
    /// same value, with no ambient reads (time, randomness, I/O) — the same
    /// contract output serializers carry. A returned `Err` is classified like
    /// a failing invocation and rides this channel's retry→DLQ ladder.
    pub fn input_with<T, P>(&self, name: &'static str, parser: P) -> EntryStream<'_, T>
    where
        T: Send + Sync + 'static,
        P: Fn(Vec<u8>) -> Result<T, String> + Send + Sync + 'static,
    {
        let wrap: crate::channel::EntryWrapFn = Arc::new(move |bytes: Vec<u8>| {
            parser(bytes).map(|v| Arc::new(v) as Arc<dyn Any + Send + Sync>)
        });
        self.entry_channel(name, wrap, true)
    }

    fn entry_channel<T: Send + Sync + 'static>(
        &self,
        name: &'static str,
        wrap: crate::channel::EntryWrapFn,
        entry_parsed: bool,
    ) -> EntryStream<'_, T> {
        let mut st = self.state.borrow_mut();
        let id = st.push_channel(name, ChannelKind::Entry);
        let entry = &mut st.channels[id as usize];
        entry.wrap_input = Some(wrap);
        entry.entry_parsed = entry_parsed;
        EntryStream {
            builder: self,
            channel_id: id,
            _phantom: PhantomData,
        }
    }
}

impl BuilderState {
    fn push_channel(&mut self, name: &'static str, kind: ChannelKind) -> u32 {
        let id = self.channels.len() as u32;
        self.channels.push(ChannelEntry {
            name,
            kind,
            producer: None,
            consumers: Vec::new(),
            plugin_name: None,
            wrap_input: None,
            entry_parsed: false,
            plugin_serializer: None,
            plugin_sink: None,
            keyed: false,
            overflow_policy: default_policy_for_kind(kind),
            retry: crate::channel::RetryPolicy::default(),
            codec: None,
        });
        id
    }

    /// Resolve or create the node for `S` under the node-identity rule: two
    /// unnamed mentions of one class are the same node; an explicit node name
    /// creates/refers to a distinct node whose identity string replaces
    /// `S::name()`. An identity claimed by a *different* declaration is a
    /// collision.
    fn register_node<S: Operation>(&mut self, node_name: Option<&'static str>) -> u32 {
        let identity = node_name.unwrap_or_else(S::name);
        let named = node_name.is_some();
        if let Some(&id) = self.operation_name_to_id.get(identity) {
            let existing = &self.operations[id as usize];
            if existing.class_id != TypeId::of::<S>() || existing.named != named {
                self.errors
                    .push(TopologyIssue::NodeIdentityCollision { name: identity });
            }
            return id;
        }
        let id = self.operations.len() as u32;

        let segment_for_erased: SegmentForFn = Arc::new(|input: &dyn Any| {
            let typed = input
                .downcast_ref::<S::Input>()
                .expect("segment_for: input type mismatch");
            S::segment_for(typed)
        });

        // Dynamic mode = overrides operation_key() with an empty operation_keys().
        // The erased operation_key closure is captured only for that shape;
        // build_topology classifies the mode and validates the
        // declare-one-or-the-other rule.
        let operation_key_erased: Option<OperationKeyFn> =
            if S::OPERATION_KEY_OVERRIDDEN && S::operation_keys().is_empty() {
                Some(Arc::new(|input: &dyn Any| {
                    let typed = input
                        .downcast_ref::<S::Input>()
                        .expect("operation_key: input type mismatch");
                    S::operation_key(typed)
                }))
            } else {
                None
            };

        let build_segment: SegmentBuilder = Box::new(
            |args: BuildSegmentArgs<'_>| -> Arc<crate::segment::SegmentSlot> {
                crate::segment::build_segment_slot::<S>(args)
            },
        );

        let (serialize_state, deserialize_state) =
            crate::operative::dispatcher::build_state_fns::<S>();

        self.operations.push(OperationEntry {
            name: identity,
            class_id: TypeId::of::<S>(),
            named,
            segment_keys: S::operation_keys(),
            segment_for_overridden: S::SEGMENT_FOR_OVERRIDDEN,
            operation_key_overridden: S::OPERATION_KEY_OVERRIDDEN,
            compensate_overridden: S::COMPENSATE_OVERRIDDEN,
            input_channel_ids: Vec::new(),
            output_channel_id: None,
            input_type_id: std::any::TypeId::of::<S::Input>(),
            output_type_id: std::any::TypeId::of::<S::Output>(),
            clock_type_name: <S::Clock as crate::operation::Clock>::type_name(),
            clock_is_event_time: <S::Clock as crate::operation::Clock>::IS_EVENT_TIME,
            change_is_nochange: std::any::TypeId::of::<S::Change>()
                == std::any::TypeId::of::<crate::operation::NoChange>(),
            mailbox_capacity: S::mailbox_capacity(),
            mailbox_capacity_overridden: S::MAILBOX_CAPACITY_OVERRIDDEN,
            build_segment,
            segment_for_erased,
            operation_key_erased,
            idle_ttl: S::segment_idle_ttl(),
            max_segments: S::max_segments(),
            serialize_state,
            deserialize_state,
        });
        self.operation_name_to_id.insert(identity, id);
        id
    }

    /// The node's output channel, created lazily on first use so a leaf node
    /// never carries a phantom channel. Named by the node's identity until a
    /// `.named(...)` override.
    fn ensure_output_channel(&mut self, node: u32) -> u32 {
        if let Some(id) = self.operations[node as usize].output_channel_id {
            return id;
        }
        let name = self.operations[node as usize].name;
        let id = self.push_channel(name, ChannelKind::InterOperation);
        self.channels[id as usize].producer = Some(node);
        self.operations[node as usize].output_channel_id = Some(id);
        id
    }

    /// Wire `channel_id` into a consumer node (created/resolved by identity).
    fn wire_consumer<S: Operation>(
        &mut self,
        channel_id: u32,
        node_name: Option<&'static str>,
    ) -> u32 {
        let consumer = self.register_node::<S>(node_name);
        let entry = &mut self.channels[channel_id as usize];
        if !entry.consumers.contains(&consumer) {
            entry.consumers.push(consumer);
        }
        let op = &mut self.operations[consumer as usize];
        if !op.input_channel_ids.contains(&channel_id) {
            op.input_channel_ids.push(channel_id);
        }
        consumer
    }
}

/// Per-kind default for the channel's `OverflowPolicy`. Entry channels
/// drop to keep the wire reader running; inter-operation channels block to
/// preserve ordering through stateful pipelines. Plugin-bound channels
/// have no SDK mailbox and the value is never consulted; we still set
/// `Block` so the field is always meaningful and to keep the descriptor
/// cheap to compare against.
pub(crate) fn default_policy_for_kind(kind: ChannelKind) -> OverflowPolicy {
    match kind {
        ChannelKind::Entry => OverflowPolicy::DropNewest,
        ChannelKind::InterOperation | ChannelKind::PluginBound => OverflowPolicy::Block,
    }
}

/// Apply a `.retry_max(...)` declaration, rejecting a zero budget at
/// registration time (surfaced by `build_topology` like every other
/// declaration error).
fn set_retry_max(st: &mut BuilderState, channel_id: u32, max_attempts: u32) {
    let entry = &mut st.channels[channel_id as usize];
    if max_attempts == 0 {
        let channel = entry.name;
        st.errors.push(TopologyIssue::RetryMaxZero { channel });
        return;
    }
    entry.retry.max_attempts = max_attempts;
}

/// Install the standard inter-op byte codec for `T` — postcard over the
/// SDK's standard serde derives, the same wire format the effect records
/// use. Shared by the `.codec()` declarations on both builder stages.
fn set_codec<T>(st: &mut BuilderState, channel_id: u32)
where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    set_codec_with::<T, _, _>(
        st,
        channel_id,
        |typed: &T| {
            postcard::to_allocvec(typed).map_err(|e| format!("inter-op codec encode failed: {e}"))
        },
        |bytes: Vec<u8>| {
            postcard::from_bytes::<T>(&bytes)
                .map_err(|e| format!("inter-op codec decode failed: {e}"))
        },
    );
}

/// Install a custom typed→bytes / bytes→typed codec pair, type-erasing both
/// halves onto the channel entry.
fn set_codec_with<T, E, D>(st: &mut BuilderState, channel_id: u32, encode: E, decode: D)
where
    T: Send + Sync + 'static,
    E: Fn(&T) -> Result<Vec<u8>, String> + Send + Sync + 'static,
    D: Fn(Vec<u8>) -> Result<T, String> + Send + Sync + 'static,
{
    let encode_erased: crate::channel::CodecEncodeFn = Arc::new(move |val: &dyn Any| {
        let typed = val
            .downcast_ref::<T>()
            .ok_or_else(|| "inter-op codec: type mismatch (build-time invariant)".to_string())?;
        encode(typed)
    });
    let decode_erased: crate::channel::EntryWrapFn = Arc::new(move |bytes: Vec<u8>| {
        decode(bytes).map(|v| Arc::new(v) as Arc<dyn Any + Send + Sync>)
    });
    st.channels[channel_id as usize].codec = Some(crate::channel::ChannelCodec {
        encode: encode_erased,
        decode: decode_erased,
    });
}

/// The default entry decode for `T`: passthrough for `Vec<u8>`, UTF-8 for
/// `String` (both byte-identical to the pre-typed-entry contract), the SDK's
/// default JSON codec otherwise. The bool is the "real parser" flag driving
/// the retry→DLQ classification of a failing decode.
fn default_entry_wrap<T: DeserializeOwned + Send + Sync + 'static>(
) -> (crate::channel::EntryWrapFn, bool) {
    if TypeId::of::<T>() == TypeId::of::<Vec<u8>>() {
        (
            Arc::new(|bytes: Vec<u8>| Ok(Arc::new(bytes) as Arc<dyn Any + Send + Sync>)),
            false,
        )
    } else if TypeId::of::<T>() == TypeId::of::<String>() {
        (
            Arc::new(|bytes: Vec<u8>| match String::from_utf8(bytes) {
                Ok(s) => Ok(Arc::new(s) as Arc<dyn Any + Send + Sync>),
                Err(e) => Err(format!("entry channel: invalid UTF-8: {}", e)),
            }),
            false,
        )
    } else {
        (
            Arc::new(|bytes: Vec<u8>| {
                serde_json::from_slice::<T>(&bytes)
                    .map(|v| Arc::new(v) as Arc<dyn Any + Send + Sync>)
                    .map_err(|e| format!("entry channel: JSON deserialization failed: {e}"))
            }),
            true,
        )
    }
}

/// The default plugin serialization for `T`: passthrough for `Vec<u8>`, UTF-8
/// bytes for `String`, the SDK's default JSON codec otherwise. Shared by
/// `output(...)` and `also_output(...)` so the two produce identical bytes for
/// the same value.
fn default_plugin_serializer<T: serde::Serialize + Send + Sync + 'static>(
) -> crate::channel::PluginSerializeFn {
    Arc::new(|val: &dyn Any| -> Vec<u8> {
        if let Some(bytes) = val.downcast_ref::<Vec<u8>>() {
            return bytes.clone();
        }
        if let Some(s) = val.downcast_ref::<String>() {
            return s.as_bytes().to_vec();
        }
        let typed = val
            .downcast_ref::<T>()
            .expect("plugin serializer: type mismatch (build-time invariant)");
        serde_json::to_vec(typed).unwrap_or_else(|e| {
            eprintln!("[flezha] default plugin serialization failed: {e}");
            Vec::new()
        })
    })
}

/// The entry channel's handle: wires the graph's root consumer via
/// [`into`](EntryStream::into) and carries the entry-side channel config.
pub struct EntryStream<'b, T> {
    builder: &'b ChannelBuilder,
    channel_id: u32,
    _phantom: PhantomData<fn() -> T>,
}

impl<'b, T> Clone for EntryStream<'b, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'b, T> Copy for EntryStream<'b, T> {}

impl<'b, T: 'static + Send + Sync> EntryStream<'b, T> {
    /// Wire the entry consumer and return **its** output stream handle, so a
    /// linear pipeline is one chained expression. Compile-time bound:
    /// `S::Input == T`.
    pub fn into<S: Operation<Input = T>>(self) -> Stream<'b, S::Output> {
        self.wire::<S>(None)
    }

    /// Like [`into`](EntryStream::into), but the consumer is a distinct named
    /// node (`sdk-graph-authoring` node-identity rule): its identity string is
    /// `node_name` instead of `S::name()`, for every purpose identity serves —
    /// state directories, journal entries, the derived channel name,
    /// diagnostics.
    pub fn into_named<S: Operation<Input = T>>(
        self,
        node_name: &'static str,
    ) -> Stream<'b, S::Output> {
        self.wire::<S>(Some(node_name))
    }

    fn wire<S: Operation<Input = T>>(
        self,
        node_name: Option<&'static str>,
    ) -> Stream<'b, S::Output> {
        let node = self
            .builder
            .state
            .borrow_mut()
            .wire_consumer::<S>(self.channel_id, node_name);
        Stream {
            builder: self.builder,
            node,
            _phantom: PhantomData,
        }
    }

    /// Override the per-kind default `OverflowPolicy`. Entry channels
    /// default to `DropNewest`; declare `Block` here only if you accept
    /// stalling the shared wire reader on this UnboundedOperative's saturation (see
    /// `docs/operative_architecture.md` for the co-tenancy hazard).
    pub fn on_full(self, policy: OverflowPolicy) -> Self {
        self.builder.state.borrow_mut().channels[self.channel_id as usize].overflow_policy = policy;
        self
    }

    /// Override the consumer-side retry budget (total attempts, default 3)
    /// applied before a failing payload is parked to the dead-letter queue.
    pub fn retry_max(self, max_attempts: u32) -> Self {
        set_retry_max(
            &mut self.builder.state.borrow_mut(),
            self.channel_id,
            max_attempts,
        );
        self
    }

    /// Override the exponential-backoff base (milliseconds, default 50)
    /// slept between failed attempts.
    pub fn retry_backoff_ms(self, base_ms: u64) -> Self {
        self.builder.state.borrow_mut().channels[self.channel_id as usize]
            .retry
            .backoff_base_ms = base_ms;
        self
    }
}

/// An operation's output stream handle, returned by `.into::<Op>()`: a
/// lightweight `Copy` reference into the operative's topology builder.
///
/// - `stream.into::<Next>()` wires Next as a consumer and returns Next's own
///   stream, so chains read in flow order.
/// - Using one handle twice — two `.into` calls on the same variable — is
///   fan-out: every consumer receives every value released on the channel.
///   [`branch`](Stream::branch) is equivalent sugar.
/// - Two different streams wiring the same (unnamed) class fan **in**: the
///   consumer node accumulates both producers' contributions.
/// - Per-channel config (`codec`, `keyed`, `on_full`, `retry_*`, `named`)
///   chains in any order relative to consumer wiring.
///
/// The handle borrows the builder, so it cannot outlive the declaration —
/// using one after the topology freezes at registration is a compile error.
pub struct Stream<'b, T> {
    builder: &'b ChannelBuilder,
    node: OperationId,
    _phantom: PhantomData<fn() -> T>,
}

impl<'b, T> Clone for Stream<'b, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'b, T> Copy for Stream<'b, T> {}

impl<'b, T: 'static + Send + Sync> Stream<'b, T> {
    fn with_channel<R>(self, f: impl FnOnce(&mut BuilderState, u32) -> R) -> R {
        let mut st = self.builder.state.borrow_mut();
        let cid = st.ensure_output_channel(self.node);
        f(&mut st, cid)
    }

    /// Wire an inter-operation consumer onto this stream's channel and return
    /// **the consumer's** output stream handle. Compile-time bound:
    /// `S::Input == T`. Calling `.into` on the same handle repeatedly declares
    /// fan-out; wiring the same (unnamed) class from two streams declares
    /// fan-in.
    pub fn into<S: Operation<Input = T>>(self) -> Stream<'b, S::Output> {
        self.wire::<S>(None)
    }

    /// Like [`into`](Stream::into), but the consumer is a distinct named node:
    /// its identity string is `node_name` instead of `S::name()`, letting one
    /// Operation class appear at several graph positions with independent
    /// state (`sdk-graph-authoring` node-identity rule).
    pub fn into_named<S: Operation<Input = T>>(
        self,
        node_name: &'static str,
    ) -> Stream<'b, S::Output> {
        self.wire::<S>(Some(node_name))
    }

    fn wire<S: Operation<Input = T>>(
        self,
        node_name: Option<&'static str>,
    ) -> Stream<'b, S::Output> {
        let node = self.with_channel(|st, cid| st.wire_consumer::<S>(cid, node_name));
        Stream {
            builder: self.builder,
            node,
            _phantom: PhantomData,
        }
    }

    /// Branching sugar: each arm receives a copy of this handle, so
    /// `s.branch([&|b| { b.into::<A>(); }, &|b| { b.into::<B>(); }])` builds
    /// exactly the topology of two `s.into` statements — nothing more. At
    /// least two arms are required (checked at registration); the two
    /// spellings are freely mixable.
    pub fn branch<const N: usize>(self, arms: [&dyn Fn(Stream<'b, T>); N]) -> Self {
        if N < 2 {
            self.builder
                .state
                .borrow_mut()
                .errors
                .push(TopologyIssue::BranchTooFewArms { arms: N });
        }
        for arm in arms {
            arm(self);
        }
        self
    }

    /// Override the auto-derived channel name (the producer node's identity)
    /// everywhere the channel name appears: wire metadata, journals, errors.
    pub fn named(self, name: &'static str) -> Self {
        self.with_channel(|st, cid| st.channels[cid as usize].name = name);
        self
    }

    /// Override the per-kind default `OverflowPolicy`. Inter-operation
    /// channels default to `Block`; switch to `DropNewest` / `DropOldest` on a
    /// per-channel basis when ordering can be sacrificed for liveness.
    pub fn on_full(self, policy: OverflowPolicy) -> Self {
        self.with_channel(|st, cid| st.channels[cid as usize].overflow_policy = policy);
        self
    }

    /// Override the consumer-side retry budget (total attempts, default 3)
    /// applied before a failing payload is parked to the dead-letter queue.
    pub fn retry_max(self, max_attempts: u32) -> Self {
        self.with_channel(|st, cid| set_retry_max(st, cid, max_attempts));
        self
    }

    /// Override the exponential-backoff base (milliseconds, default 50)
    /// slept between failed attempts.
    pub fn retry_backoff_ms(self, base_ms: u64) -> Self {
        self.with_channel(|st, cid| st.channels[cid as usize].retry.backoff_base_ms = base_ms);
        self
    }

    /// Declare the standard inter-op byte codec (postcard over `T`'s serde
    /// derives) so this channel's handoff batch is journal-able at the
    /// bounded op-commit barrier and re-seedable on crash-resume. Required
    /// on every inter-op channel of a durable structured bounded class.
    pub fn codec(self) -> Self
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        self.with_channel(|st, cid| set_codec::<T>(st, cid));
        self
    }

    /// Declare a custom typed→bytes / bytes→typed codec pair for payload
    /// types without the standard serde derives.
    pub fn codec_with<E, D>(self, encode: E, decode: D) -> Self
    where
        E: Fn(&T) -> Result<Vec<u8>, String> + Send + Sync + 'static,
        D: Fn(Vec<u8>) -> Result<T, String> + Send + Sync + 'static,
    {
        self.with_channel(|st, cid| set_codec_with::<T, E, D>(st, cid, encode, decode));
        self
    }

    /// Opt this channel into output identity: every plugin emit must carry
    /// a natural key via `ctx.emit_keyed(key, value)` and the runtime
    /// stamps `flz.segment` / `flz.seq` / `flz.key` on the wire frame.
    /// Requires a plugin wire (`output` or `also_output_with`);
    /// `build_topology` rejects a keyed channel without one.
    pub fn keyed(self) -> Self {
        self.with_channel(|st, cid| st.channels[cid as usize].keyed = true);
        self
    }

    /// Attach a plugin sink alongside inter-operation fan-out. The channel's
    /// `kind` stays `InterOperation`, further `.into::<S>()` consumer wiring
    /// chains naturally, and a typed value emitted on this channel
    /// reaches both the consumer mailboxes (typed) and the named plugin
    /// (serialized to bytes via `serializer`). The serializer is invoked
    /// once per emit regardless of consumer count.
    ///
    /// Calling `also_output_with`/`also_output` twice on the same channel is a
    /// registration error (`TopologyIssue::DuplicatePluginSink`);
    /// the first call wins.
    pub fn also_output_with<F>(self, plugin_name: &'static str, serializer: F) -> Self
    where
        F: Fn(&T) -> Vec<u8> + Send + Sync + 'static,
    {
        let serializer_erased: crate::channel::PluginSerializeFn =
            Arc::new(move |val: &dyn Any| -> Vec<u8> {
                let typed = val
                    .downcast_ref::<T>()
                    .expect("hybrid plugin serializer: type mismatch (build-time invariant)");
                serializer(typed)
            });
        self.attach_plugin_sink(plugin_name, serializer_erased)
    }

    /// Like [`also_output_with`](Stream::also_output_with) with the default
    /// serialization rules of [`output`](Stream::output): passthrough for
    /// `Vec<u8>`, UTF-8 for `String`, the SDK's default JSON codec otherwise.
    pub fn also_output(self, plugin_name: &'static str) -> Self
    where
        T: serde::Serialize,
    {
        self.attach_plugin_sink(plugin_name, default_plugin_serializer::<T>())
    }

    fn attach_plugin_sink(
        self,
        plugin_name: &'static str,
        serializer: crate::channel::PluginSerializeFn,
    ) -> Self {
        self.with_channel(|st, cid| {
            let entry = &mut st.channels[cid as usize];
            if entry.plugin_sink.is_some() {
                let channel = entry.name;
                st.errors
                    .push(TopologyIssue::DuplicatePluginSink { channel });
                return;
            }
            entry.plugin_sink = Some(PluginSink {
                name: plugin_name,
                serializer,
            });
        });
        self
    }

    /// Close this stream plugin-bound: the operative declares a named exit and
    /// the controller's plugin layer receives the values, serialized with the
    /// default rules — passthrough for `Vec<u8>`, UTF-8 for `String`, the
    /// SDK's default JSON codec otherwise. Terminal: the returned [`Channel`]
    /// exposes no further builder methods.
    pub fn output(self, plugin_name: &'static str) -> Channel<T>
    where
        T: serde::Serialize,
    {
        self.with_channel(|st, cid| {
            let entry = &mut st.channels[cid as usize];
            entry.kind = ChannelKind::PluginBound;
            entry.plugin_name = Some(plugin_name);
            entry.plugin_serializer = Some(default_plugin_serializer::<T>());
            Channel::new(cid)
        })
    }
}
