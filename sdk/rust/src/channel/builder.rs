//! Fluent channel builder per `operative-mvp-rust/design.md` Decision 2.
//!
//! Compile-time enforcement:
//! - `c.channel::<T>(...)` returns a `ChannelDecl<T>`. `.from::<S>()` requires
//!   `S: Operation<Output = T>` — type mismatch fires at the call site.
//! - `.into::<S>()` requires `S: Operation<Input = T>` — same.
//! - `.from` returns `ChannelDeclWithProducer` which has no further `.from`
//!   method, so multi-producer is a compile error.
//! - Entry channels accept only `Vec<u8>` / `String` via the sealed
//!   `EntryChannelType` trait (umbrella Decision 12).

use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::channel::entry_type::EntryChannelType;
use crate::channel::{Channel, ChannelKind, OperationId, OverflowPolicy};
use crate::operation::Operation;

/// Type-erased dispatcher factory for one Operation. Stored on
/// `ChannelBuilder::operations` and consumed by `build_topology` to produce
/// per-segment slots in each `UnboundedInstance`.
pub(crate) struct OperationEntry {
    pub name: &'static str,
    pub segment_keys: Vec<&'static str>,
    pub segment_for_overridden: bool,
    pub operation_key_overridden: bool,
    /// `Operation::COMPENSATE_OVERRIDDEN` captured at registration time. The
    /// bounded driver consults the resolved descriptor flag to fire saga
    /// compensation only for operations that declared a `compensate` hook.
    pub compensate_overridden: bool,
    pub input_channel_id: Option<u32>,
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
/// `also_to_plugin_with(...)`. The presence of this field flips the
/// channel from pure inter-operation fan-out to hybrid (consumers + plugin
/// wire) without changing `ChannelKind`.
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
    pub plugin_serializer: Option<crate::channel::PluginSerializeFn>,
    /// Hybrid plugin sink: populated by `also_to_plugin_with(...)` on
    /// an `InterOperation` channel. Mutually exclusive with
    /// `plugin_name`/`plugin_serializer`, which are populated by
    /// `to_plugin(...)` on a pure `PluginBound` channel.
    pub plugin_sink: Option<PluginSink>,
    /// Output-identity opt-in set by `.keyed()`. Valid only on channels
    /// with a plugin wire; `build_topology` rejects a keyed channel that
    /// has neither `to_plugin` nor `also_to_plugin_with`.
    pub keyed: bool,
    /// Backpressure policy applied at the push site when the consumer
    /// mailbox is full. Initialized at `declare_channel` time per
    /// channel kind (Entry → DropNewest, InterOperation → Block); plugin-bound
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
pub enum BuildTopologyError {
    #[error("channel `{0}` declared more than once on this operative")]
    DuplicateChannel(&'static str),

    #[error("channel `{name}` was declared but never wired with .from(...)")]
    UnwiredChannel { name: &'static str },

    #[error("channel `{name}` already has a producer; multi-producer is not allowed")]
    MultiProducer { name: &'static str },

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
        "channel `{channel}` was declared with `also_to_plugin_with(...)` more than once; \
         only one plugin sink per channel is allowed"
    )]
    DuplicatePluginSink { channel: &'static str },

    #[error(
        "channel `{channel}` is declared .keyed() but has no plugin wire; output identity \
         stamps only exist on plugin-bound frames (add .to_plugin(...) or \
         .also_to_plugin_with(...))"
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

/// Builder collected by `UnboundedOperative::channels`. Internal state turns into a
/// frozen `OperativeTopology` at `build_topology` time.
pub struct ChannelBuilder {
    pub(crate) channels: Vec<ChannelEntry>,
    pub(crate) name_to_id: HashMap<&'static str, u32>,
    pub(crate) operations: Vec<OperationEntry>,
    pub(crate) operation_name_to_id: HashMap<&'static str, u32>,
    pub(crate) errors: Vec<BuildTopologyError>,
    pub(crate) effectively_once: bool,
    pub(crate) correction: Option<crate::operative::topology::CorrectionPolicy>,
    pub(crate) ack_granularity: crate::operative::topology::AckGranularity,
}

impl ChannelBuilder {
    pub(crate) fn new() -> Self {
        Self {
            channels: Vec::new(),
            name_to_id: HashMap::new(),
            operations: Vec::new(),
            operation_name_to_id: HashMap::new(),
            errors: Vec::new(),
            effectively_once: false,
            correction: None,
            ack_granularity: crate::operative::topology::AckGranularity::PerInput,
        }
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
    pub fn correction_window(&mut self, window_ms: u64) {
        let beyond_window = self.correction.map(|c| c.beyond_window).unwrap_or_default();
        self.correction = Some(crate::operative::topology::CorrectionPolicy {
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
    pub fn beyond_window_policy(&mut self, policy: crate::operative::topology::BeyondWindowPolicy) {
        let window_ms = self.correction.map(|c| c.window_ms).unwrap_or(0);
        self.correction = Some(crate::operative::topology::CorrectionPolicy {
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
    pub fn effectively_once(&mut self) {
        self.effectively_once = true;
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
    pub fn checkpoint_ack(&mut self) {
        self.ack_granularity = crate::operative::topology::AckGranularity::Checkpoint;
    }

    /// Declare an entry channel fed by the controller. T is restricted to
    /// `Vec<u8>` or `String` by the sealed `EntryChannelType` trait.
    pub fn input<T: EntryChannelType + EntryPayloadWrap>(
        &mut self,
        name: &'static str,
    ) -> EntryChannel<'_, T> {
        let id = self.declare_channel(name, ChannelKind::Entry);
        let wrap_fn: crate::channel::EntryWrapFn = Arc::new(|bytes: Vec<u8>| T::wrap(bytes));
        self.channels[id as usize].wrap_input = Some(wrap_fn);
        EntryChannel {
            builder: self,
            channel_id: id,
            name,
            _phantom: PhantomData,
        }
    }

    /// Declare an inter-operation or plugin-bound channel. The returned
    /// `ChannelDecl` requires `.from::<S>()` to identify a producer.
    pub fn channel<T: 'static + Send + Sync>(&mut self, name: &'static str) -> ChannelDecl<'_, T> {
        let id = self.declare_channel(name, ChannelKind::InterOperation);
        ChannelDecl {
            builder: self,
            channel_id: id,
            name,
            _phantom: PhantomData,
        }
    }

    fn declare_channel(&mut self, name: &'static str, kind: ChannelKind) -> u32 {
        if self.name_to_id.contains_key(name) {
            self.errors.push(BuildTopologyError::DuplicateChannel(name));
        }
        let id = self.channels.len() as u32;
        self.channels.push(ChannelEntry {
            name,
            kind,
            producer: None,
            consumers: Vec::new(),
            plugin_name: None,
            wrap_input: None,
            plugin_serializer: None,
            plugin_sink: None,
            keyed: false,
            overflow_policy: default_policy_for_kind(kind),
            retry: crate::channel::RetryPolicy::default(),
            codec: None,
        });
        self.name_to_id.insert(name, id);
        id
    }

    /// Register a Operation if it isn't already in the table. Returns its
    /// OperationId. Idempotent — wiring a Operation as both consumer and producer
    /// across two channels reuses the same entry.
    fn register_operation<S: Operation>(&mut self) -> u32 {
        if let Some(&id) = self.operation_name_to_id.get(S::name()) {
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
            name: S::name(),
            segment_keys: S::operation_keys(),
            segment_for_overridden: S::SEGMENT_FOR_OVERRIDDEN,
            operation_key_overridden: S::OPERATION_KEY_OVERRIDDEN,
            compensate_overridden: S::COMPENSATE_OVERRIDDEN,
            input_channel_id: None,
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
        self.operation_name_to_id.insert(S::name(), id);
        id
    }
}

/// Per-kind default for the channel's `OverflowPolicy`. Entry channels
/// drop to keep the wire reader running; inter-operation channels block to
/// preserve ordering through stateful pipelines. Plugin-bound channels
/// have no SDK mailbox and the value is never consulted; we still set
/// `Block` so the field is always meaningful and to keep the descriptor
/// cheap to compare against.
/// Apply a `.retry_max(...)` declaration, rejecting a zero budget at
/// registration time (surfaced by `build_topology` like every other
/// declaration error).
fn set_retry_max(builder: &mut ChannelBuilder, channel_id: u32, max_attempts: u32) {
    let entry = &mut builder.channels[channel_id as usize];
    if max_attempts == 0 {
        builder.errors.push(BuildTopologyError::RetryMaxZero {
            channel: entry.name,
        });
        return;
    }
    entry.retry.max_attempts = max_attempts;
}

/// Install the standard inter-op byte codec for `T` — postcard over the
/// SDK's standard serde derives, the same wire format the effect records
/// use. Shared by the `.codec()` declarations on both builder stages.
fn set_codec<T>(builder: &mut ChannelBuilder, channel_id: u32)
where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    set_codec_with::<T, _, _>(
        builder,
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
fn set_codec_with<T, E, D>(builder: &mut ChannelBuilder, channel_id: u32, encode: E, decode: D)
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
    builder.channels[channel_id as usize].codec = Some(crate::channel::ChannelCodec {
        encode: encode_erased,
        decode: decode_erased,
    });
}

pub(crate) fn default_policy_for_kind(kind: ChannelKind) -> OverflowPolicy {
    match kind {
        ChannelKind::Entry => OverflowPolicy::DropNewest,
        ChannelKind::InterOperation | ChannelKind::PluginBound => OverflowPolicy::Block,
    }
}

/// Entry-channel declaration. Only exposes `.into::<X>()` to wire a
/// consumer Operation; entry channels are fed by the controller so there is no
/// `.from(...)`.
pub struct EntryChannel<'b, T> {
    builder: &'b mut ChannelBuilder,
    channel_id: u32,
    name: &'static str,
    _phantom: PhantomData<fn() -> T>,
}

impl<'b, T: 'static + Send + Sync> EntryChannel<'b, T> {
    /// Wire the entry-operation consumer. Compile-time bound: `S::Input == T`.
    pub fn into<S: Operation<Input = T>>(self) -> Channel<T> {
        let operation_id = self.builder.register_operation::<S>();
        let entry = &mut self.builder.channels[self.channel_id as usize];
        entry.consumers.push(operation_id);

        let operation = &mut self.builder.operations[operation_id as usize];
        operation.input_channel_id = Some(self.channel_id);

        Channel::new(self.channel_id)
    }

    /// Override the per-kind default `OverflowPolicy`. Entry channels
    /// default to `DropNewest`; declare `Block` here only if you accept
    /// stalling the shared wire reader on this UnboundedOperative's saturation (see
    /// `docs/operative_architecture.md` for the co-tenancy hazard).
    pub fn on_full(self, policy: OverflowPolicy) -> Self {
        self.builder.channels[self.channel_id as usize].overflow_policy = policy;
        self
    }

    /// Override the consumer-side retry budget (total attempts, default 3)
    /// applied before a failing payload is parked to the dead-letter queue.
    pub fn retry_max(self, max_attempts: u32) -> Self {
        set_retry_max(self.builder, self.channel_id, max_attempts);
        self
    }

    /// Override the exponential-backoff base (milliseconds, default 50)
    /// slept between failed attempts.
    pub fn retry_backoff_ms(self, base_ms: u64) -> Self {
        self.builder.channels[self.channel_id as usize]
            .retry
            .backoff_base_ms = base_ms;
        self
    }

    /// Discarded handle accessor (rarely useful — kept for symmetry).
    pub fn name(&self) -> &'static str {
        self.name
    }
}

/// Inter-operation / plugin-bound channel awaiting a producer.
pub struct ChannelDecl<'b, T> {
    builder: &'b mut ChannelBuilder,
    channel_id: u32,
    name: &'static str,
    _phantom: PhantomData<fn() -> T>,
}

impl<'b, T: 'static + Send + Sync> ChannelDecl<'b, T> {
    /// Override the per-kind default `OverflowPolicy` before wiring the
    /// producer. Inter-operation channels default to `Block`; switch to
    /// `DropNewest` / `DropOldest` on a per-channel basis when ordering
    /// can be sacrificed for liveness.
    pub fn on_full(self, policy: OverflowPolicy) -> Self {
        self.builder.channels[self.channel_id as usize].overflow_policy = policy;
        self
    }

    /// Override the consumer-side retry budget (total attempts, default 3)
    /// applied before a failing payload is parked to the dead-letter queue.
    pub fn retry_max(self, max_attempts: u32) -> Self {
        set_retry_max(self.builder, self.channel_id, max_attempts);
        self
    }

    /// Override the exponential-backoff base (milliseconds, default 50)
    /// slept between failed attempts.
    pub fn retry_backoff_ms(self, base_ms: u64) -> Self {
        self.builder.channels[self.channel_id as usize]
            .retry
            .backoff_base_ms = base_ms;
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
        set_codec::<T>(self.builder, self.channel_id);
        self
    }

    /// Declare a custom typed→bytes / bytes→typed codec pair for payload
    /// types without the standard serde derives.
    pub fn codec_with<E, D>(self, encode: E, decode: D) -> Self
    where
        E: Fn(&T) -> Result<Vec<u8>, String> + Send + Sync + 'static,
        D: Fn(Vec<u8>) -> Result<T, String> + Send + Sync + 'static,
    {
        set_codec_with::<T, E, D>(self.builder, self.channel_id, encode, decode);
        self
    }

    /// Wire the producer Operation. Compile-time bound: `S::Output == T`.
    /// Multi-producer is a compile error: this consumes `self` and returns
    /// `ChannelDeclWithProducer`, which has no `.from` method.
    pub fn from<S: Operation<Output = T>>(self) -> ChannelDeclWithProducer<'b, T> {
        let operation_id = self.builder.register_operation::<S>();

        // Defense-in-depth: detect attempts to set the producer twice
        // (the type system already prevents this on a single declaration
        // chain, but two `c.channel::<T>(name)` calls with the same name
        // would slip through).
        let entry = &mut self.builder.channels[self.channel_id as usize];
        if entry.producer.is_some() {
            self.builder
                .errors
                .push(BuildTopologyError::MultiProducer { name: self.name });
        } else {
            entry.producer = Some(operation_id);
        }
        let operation = &mut self.builder.operations[operation_id as usize];
        operation.output_channel_id = Some(self.channel_id);

        ChannelDeclWithProducer {
            builder: self.builder,
            channel_id: self.channel_id,
            _phantom: PhantomData,
        }
    }
}

/// Channel with a producer registered. Accepts zero or more `.into::<X>()`
/// inter-operation consumers, or one `.to_plugin(...)` plugin destination.
pub struct ChannelDeclWithProducer<'b, T> {
    builder: &'b mut ChannelBuilder,
    channel_id: u32,
    _phantom: PhantomData<fn() -> T>,
}

impl<'b, T: 'static + Send + Sync> ChannelDeclWithProducer<'b, T> {
    /// Override the per-kind default `OverflowPolicy`. Available only
    /// before `.to_plugin(...)`; the resulting `Channel<T>` handle has
    /// no `.on_full(...)` method, so plugin-bound declarations are a
    /// compile error.
    pub fn on_full(self, policy: OverflowPolicy) -> Self {
        self.builder.channels[self.channel_id as usize].overflow_policy = policy;
        self
    }

    /// Override the consumer-side retry budget (total attempts, default 3)
    /// applied before a failing payload is parked to the dead-letter queue.
    pub fn retry_max(self, max_attempts: u32) -> Self {
        set_retry_max(self.builder, self.channel_id, max_attempts);
        self
    }

    /// Override the exponential-backoff base (milliseconds, default 50)
    /// slept between failed attempts.
    pub fn retry_backoff_ms(self, base_ms: u64) -> Self {
        self.builder.channels[self.channel_id as usize]
            .retry
            .backoff_base_ms = base_ms;
        self
    }

    /// Declare the standard inter-op byte codec (postcard over `T`'s serde
    /// derives). See [`ChannelDecl::codec`] — declarable on either side of
    /// `.from(...)`.
    pub fn codec(self) -> Self
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        set_codec::<T>(self.builder, self.channel_id);
        self
    }

    /// Declare a custom typed→bytes / bytes→typed codec pair. See
    /// [`ChannelDecl::codec_with`].
    pub fn codec_with<E, D>(self, encode: E, decode: D) -> Self
    where
        E: Fn(&T) -> Result<Vec<u8>, String> + Send + Sync + 'static,
        D: Fn(Vec<u8>) -> Result<T, String> + Send + Sync + 'static,
    {
        set_codec_with::<T, E, D>(self.builder, self.channel_id, encode, decode);
        self
    }

    /// Wire an inter-operation consumer. Compile-time bound: `S::Input == T`.
    /// Calling `.into` repeatedly is allowed (fan-out).
    pub fn into<S: Operation<Input = T>>(self) -> Self {
        let operation_id = self.builder.register_operation::<S>();
        let entry = &mut self.builder.channels[self.channel_id as usize];
        entry.consumers.push(operation_id);
        let operation = &mut self.builder.operations[operation_id as usize];
        operation.input_channel_id = Some(self.channel_id);
        self
    }

    /// Attach a plugin sink alongside inter-operation fan-out. The channel's
    /// `kind` stays `InterOperation`, further `.into::<S>()` consumer wiring
    /// chains naturally, and a typed value emitted on this channel
    /// reaches both the consumer mailboxes (typed) and the named plugin
    /// (serialized to bytes via `serializer`). The serializer is invoked
    /// once per emit regardless of consumer count.
    ///
    /// Calling `also_to_plugin_with` twice on the same channel is a
    /// registration error (`BuildTopologyError::DuplicatePluginSink`);
    /// the first call wins.
    pub fn also_to_plugin_with<F>(self, plugin_name: &'static str, serializer: F) -> Self
    where
        F: Fn(&T) -> Vec<u8> + Send + Sync + 'static,
    {
        let entry = &mut self.builder.channels[self.channel_id as usize];
        if entry.plugin_sink.is_some() {
            self.builder
                .errors
                .push(BuildTopologyError::DuplicatePluginSink {
                    channel: entry.name,
                });
            return self;
        }
        let serializer_erased: crate::channel::PluginSerializeFn =
            Arc::new(move |val: &dyn Any| -> Vec<u8> {
                let typed = val
                    .downcast_ref::<T>()
                    .expect("hybrid plugin serializer: type mismatch (build-time invariant)");
                serializer(typed)
            });
        entry.plugin_sink = Some(PluginSink {
            name: plugin_name,
            serializer: serializer_erased,
        });
        self
    }

    /// Opt this channel into output identity: every plugin emit must carry
    /// a natural key via `ctx.emit_keyed(key, value)` and the runtime
    /// stamps `flz.segment` / `flz.seq` / `flz.key` on the wire frame.
    /// Requires a plugin wire (`to_plugin` or `also_to_plugin_with`);
    /// `build_topology` rejects a keyed channel without one.
    pub fn keyed(self) -> Self {
        self.builder.channels[self.channel_id as usize].keyed = true;
        self
    }

    /// Mark this channel as plugin-bound. The controller's plugin layer
    /// receives the values. v1 requires `T: AsRef<[u8]>` (`Vec<u8>` /
    /// `String`); the producer Operation is responsible for serializing
    /// typed messages to bytes before emit.
    pub fn to_plugin(self, plugin_name: &'static str) -> Channel<T>
    where
        T: AsRef<[u8]>,
    {
        let entry = &mut self.builder.channels[self.channel_id as usize];
        entry.kind = ChannelKind::PluginBound;
        entry.plugin_name = Some(plugin_name);
        let serializer: crate::channel::PluginSerializeFn = Arc::new(|val: &dyn Any| -> Vec<u8> {
            val.downcast_ref::<T>()
                .expect("plugin_serializer: type mismatch")
                .as_ref()
                .to_vec()
        });
        entry.plugin_serializer = Some(serializer);
        Channel::new(self.channel_id)
    }
}

/// Helper trait that turns wire bytes into a typed `Arc<dyn Any +
/// Send + Sync>`. Closed: only `Vec<u8>` and `String` impls in v1.
pub trait EntryPayloadWrap: 'static + Send + Sync + Sized {
    fn wrap(bytes: Vec<u8>) -> Result<Arc<dyn Any + Send + Sync>, String>;
}

impl EntryPayloadWrap for Vec<u8> {
    fn wrap(bytes: Vec<u8>) -> Result<Arc<dyn Any + Send + Sync>, String> {
        Ok(Arc::new(bytes))
    }
}

impl EntryPayloadWrap for String {
    fn wrap(bytes: Vec<u8>) -> Result<Arc<dyn Any + Send + Sync>, String> {
        match String::from_utf8(bytes) {
            Ok(s) => Ok(Arc::new(s)),
            Err(e) => Err(format!("entry channel: invalid UTF-8: {}", e)),
        }
    }
}
