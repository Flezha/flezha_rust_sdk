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
/// per-segment slots in each `ContinuousInstance`.
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
    pub instance: std::sync::Weak<crate::operative::instance::ContinuousInstance>,
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
    /// `ContinuousInstance`. The `SegmentSlot` carries this on its
    /// `persistence_enabled` field; the per-process flusher inspects it
    /// at drain time to decide whether to call `send_frame`.
    pub persistence_enabled: bool,
    pub pool: Arc<crate::executor::work_stealing_pool::WorkStealingPool>,
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
    /// Backpressure policy applied at the push site when the consumer
    /// mailbox is full. Initialized at `declare_channel` time per
    /// channel kind (Entry → DropNewest, InterOperation → Block); plugin-bound
    /// declarations carry a placeholder that is never consulted because
    /// `emit_plugin` writes straight to the wire. `.on_full(...)`
    /// overwrites this value.
    pub overflow_policy: OverflowPolicy,
}

/// Reasons `ContinuousOperative::channels(c)` declarations fail at registration time.
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
}

/// Builder collected by `ContinuousOperative::channels`. Internal state turns into a
/// frozen `OperativeTopology` at `build_topology` time.
pub struct ChannelBuilder {
    pub(crate) channels: Vec<ChannelEntry>,
    pub(crate) name_to_id: HashMap<&'static str, u32>,
    pub(crate) operations: Vec<OperationEntry>,
    pub(crate) operation_name_to_id: HashMap<&'static str, u32>,
    pub(crate) errors: Vec<BuildTopologyError>,
}

impl ChannelBuilder {
    pub(crate) fn new() -> Self {
        Self {
            channels: Vec::new(),
            name_to_id: HashMap::new(),
            operations: Vec::new(),
            operation_name_to_id: HashMap::new(),
            errors: Vec::new(),
        }
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
            overflow_policy: default_policy_for_kind(kind),
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
    /// stalling the shared wire reader on this ContinuousOperative's saturation (see
    /// `docs/operative_architecture.md` for the co-tenancy hazard).
    pub fn on_full(self, policy: OverflowPolicy) -> Self {
        self.builder.channels[self.channel_id as usize].overflow_policy = policy;
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
