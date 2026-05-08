//! Fluent channel builder per `relay-mvp-rust/design.md` Decision 2.
//!
//! Compile-time enforcement:
//! - `c.channel::<T>(...)` returns a `ChannelDecl<T>`. `.from::<S>()` requires
//!   `S: Shard<Output = T>` — type mismatch fires at the call site.
//! - `.into::<S>()` requires `S: Shard<Input = T>` — same.
//! - `.from` returns `ChannelDeclWithProducer` which has no further `.from`
//!   method, so multi-producer is a compile error.
//! - Entry channels accept only `Vec<u8>` / `String` via the sealed
//!   `EntryChannelType` trait (umbrella Decision 12).

use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::channel::entry_type::EntryChannelType;
use crate::channel::{Channel, ChannelKind, OverflowPolicy, ShardId};
use crate::shard::Shard;

/// Type-erased dispatcher factory for one Shard. Stored on
/// `ChannelBuilder::shards` and consumed by `build_topology` to produce
/// per-partition slots in each `RelayInstance`.
pub(crate) struct ShardEntry {
    pub name: &'static str,
    pub partition_keys: Vec<&'static str>,
    pub partition_for_overridden: bool,
    pub shard_key_overridden: bool,
    pub input_channel_id: Option<u32>,
    pub output_channel_id: Option<u32>,
    /// Erased input-type marker: matches the consumer-edge's
    /// `consumer_input_type_id` for compile-time-erased correctness checks.
    pub input_type_id: std::any::TypeId,
    /// Erased output-type marker: matches the producer-edge's payload type.
    pub output_type_id: std::any::TypeId,

    /// `Shard::mailbox_capacity()` captured at registration time. The
    /// resolved value (after applying env-var precedence in `build_topology`)
    /// is the size of every Partition mailbox built for this Shard.
    pub mailbox_capacity: usize,

    /// `Shard::MAILBOX_CAPACITY_OVERRIDDEN` captured at registration time.
    /// Drives the precedence rule in `resolve_mailbox_capacity`.
    pub mailbox_capacity_overridden: bool,

    /// Build a per-partition slot. The closure captures the Shard type
    /// statically so the partition mailbox carries `Arc<S::Input>` items
    /// and `Shard::run` is called with the typed signature.
    pub build_partition: PartitionBuilder,

    /// Partition routing for inputs targeting this Shard. Returns the
    /// partition keys. Single-mode shards return an empty vec; the runtime
    /// then routes everything to partition index 0.
    pub partition_for_erased: PartitionForFn,
}

pub(crate) type PartitionForFn =
    Arc<dyn Fn(&dyn Any) -> Vec<&'static str> + Send + Sync + 'static>;

pub(crate) type PartitionBuilder = Box<
    dyn Fn(BuildPartitionArgs<'_>) -> Arc<crate::partition::PartitionSlot>
        + Send
        + Sync
        + 'static,
>;

pub struct BuildPartitionArgs<'a> {
    pub topology: Arc<crate::relay::topology::RelayTopology>,
    pub instance: std::sync::Weak<crate::relay::instance::RelayInstance>,
    pub shard_id: ShardId,
    pub shard_key: Option<&'a str>,
    /// Zero-based partition index within the Shard. `0` for single-mode
    /// shards; `0..shard_keys().len()` for static-mode shards.
    pub partition_index: usize,
    /// Frozen-at-instance-start persistence flag, propagated from the
    /// `RelayInstance`. The `PartitionSlot` carries this on its
    /// `persistence_enabled` field; the per-process flusher inspects it
    /// at drain time to decide whether to call `send_frame`.
    pub persistence_enabled: bool,
    pub pool: Arc<crate::executor::work_stealing_pool::WorkStealingPool>,
}

/// Internal channel record used during topology construction.
pub(crate) struct ChannelEntry {
    pub name: &'static str,
    pub kind: ChannelKind,
    pub producer: Option<ShardId>,
    pub consumers: Vec<ShardId>,
    pub plugin_name: Option<&'static str>,
    pub payload_type_id: std::any::TypeId,
    pub wrap_input: Option<crate::channel::EntryWrapFn>,
    pub plugin_serializer: Option<crate::channel::PluginSerializeFn>,
    /// Backpressure policy applied at the push site when the consumer
    /// mailbox is full. Initialized at `declare_channel` time per
    /// channel kind (Entry → DropNewest, InterShard → Block); plugin-bound
    /// declarations carry a placeholder that is never consulted because
    /// `emit_plugin` writes straight to the wire. `.on_full(...)`
    /// overwrites this value.
    pub overflow_policy: OverflowPolicy,
}

/// Reasons `Relay::channels(c)` declarations fail at registration time.
#[derive(Debug, thiserror::Error)]
pub enum BuildTopologyError {
    #[error("channel `{0}` declared more than once on this relay")]
    DuplicateChannel(&'static str),

    #[error("channel `{name}` was declared but never wired with .from(...)")]
    UnwiredChannel { name: &'static str },

    #[error("channel `{name}` already has a producer; multi-producer is not allowed")]
    MultiProducer { name: &'static str },

    #[error(
        "shard `{name}` declares non-empty shard_keys() but does not override partition_for \
         (set PARTITION_FOR_OVERRIDDEN = true)"
    )]
    PartitionForMissing { name: &'static str },

    #[error(
        "shard `{name}` overrides shard_key() (dynamic-mode marker) without declaring \
         shard_keys(); dynamic mode is reserved for `relay-dynamic-partitioning` and \
         rejected in v1"
    )]
    DynamicModeOnly { name: &'static str },

    #[error("shard `{name}` registered with conflicting input/output types")]
    TypeMismatch { name: &'static str },

    #[error(
        "shard `{shard}` declared mailbox_capacity = 0; capacity must be in 1..=1_048_576"
    )]
    MailboxCapacityZero { shard: &'static str },

    #[error(
        "shard `{shard}` declared mailbox_capacity = {value}; capacity must be in 1..=1_048_576"
    )]
    MailboxCapacityTooLarge { shard: &'static str, value: usize },
}

/// Builder collected by `Relay::channels`. Internal state turns into a
/// frozen `RelayTopology` at `build_topology` time.
pub struct ChannelBuilder {
    pub(crate) channels: Vec<ChannelEntry>,
    pub(crate) name_to_id: HashMap<&'static str, u32>,
    pub(crate) shards: Vec<ShardEntry>,
    pub(crate) shard_name_to_id: HashMap<&'static str, u32>,
    pub(crate) errors: Vec<BuildTopologyError>,
}

impl ChannelBuilder {
    pub(crate) fn new() -> Self {
        Self {
            channels: Vec::new(),
            name_to_id: HashMap::new(),
            shards: Vec::new(),
            shard_name_to_id: HashMap::new(),
            errors: Vec::new(),
        }
    }

    /// Declare an entry channel fed by the controller. T is restricted to
    /// `Vec<u8>` or `String` by the sealed `EntryChannelType` trait.
    pub fn input<T: EntryChannelType + EntryPayloadWrap>(
        &mut self,
        name: &'static str,
    ) -> EntryChannel<'_, T> {
        let id = self.declare_channel::<T>(name, ChannelKind::Entry);
        let wrap_fn: crate::channel::EntryWrapFn = Arc::new(|bytes: Vec<u8>| T::wrap(bytes));
        self.channels[id as usize].wrap_input = Some(wrap_fn);
        EntryChannel {
            builder: self,
            channel_id: id,
            name,
            _phantom: PhantomData,
        }
    }

    /// Declare an inter-shard or plugin-bound channel. The returned
    /// `ChannelDecl` requires `.from::<S>()` to identify a producer.
    pub fn channel<T: 'static + Send + Sync>(
        &mut self,
        name: &'static str,
    ) -> ChannelDecl<'_, T> {
        let id = self.declare_channel::<T>(name, ChannelKind::InterShard);
        ChannelDecl {
            builder: self,
            channel_id: id,
            name,
            _phantom: PhantomData,
        }
    }

    fn declare_channel<T: 'static>(&mut self, name: &'static str, kind: ChannelKind) -> u32 {
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
            payload_type_id: std::any::TypeId::of::<T>(),
            wrap_input: None,
            plugin_serializer: None,
            overflow_policy: default_policy_for_kind(kind),
        });
        self.name_to_id.insert(name, id);
        id
    }

    /// Register a Shard if it isn't already in the table. Returns its
    /// ShardId. Idempotent — wiring a Shard as both consumer and producer
    /// across two channels reuses the same entry.
    fn register_shard<S: Shard>(&mut self) -> u32 {
        if let Some(&id) = self.shard_name_to_id.get(S::name()) {
            return id;
        }
        let id = self.shards.len() as u32;

        let partition_for_erased: PartitionForFn = Arc::new(|input: &dyn Any| {
            let typed = input
                .downcast_ref::<S::Input>()
                .expect("partition_for: input type mismatch");
            S::partition_for(typed)
        });

        let build_partition: PartitionBuilder =
            Box::new(|args: BuildPartitionArgs<'_>| -> Arc<crate::partition::PartitionSlot> {
                crate::partition::build_partition_slot::<S>(args)
            });

        self.shards.push(ShardEntry {
            name: S::name(),
            partition_keys: S::shard_keys(),
            partition_for_overridden: S::PARTITION_FOR_OVERRIDDEN,
            shard_key_overridden: S::SHARD_KEY_OVERRIDDEN,
            input_channel_id: None,
            output_channel_id: None,
            input_type_id: std::any::TypeId::of::<S::Input>(),
            output_type_id: std::any::TypeId::of::<S::Output>(),
            mailbox_capacity: S::mailbox_capacity(),
            mailbox_capacity_overridden: S::MAILBOX_CAPACITY_OVERRIDDEN,
            build_partition,
            partition_for_erased,
        });
        self.shard_name_to_id.insert(S::name(), id);
        id
    }
}

/// Per-kind default for the channel's `OverflowPolicy`. Entry channels
/// drop to keep the wire reader running; inter-shard channels block to
/// preserve ordering through stateful pipelines. Plugin-bound channels
/// have no SDK mailbox and the value is never consulted; we still set
/// `Block` so the field is always meaningful and to keep the descriptor
/// cheap to compare against.
pub(crate) fn default_policy_for_kind(kind: ChannelKind) -> OverflowPolicy {
    match kind {
        ChannelKind::Entry => OverflowPolicy::DropNewest,
        ChannelKind::InterShard | ChannelKind::PluginBound => OverflowPolicy::Block,
    }
}

/// Entry-channel declaration. Only exposes `.into::<X>()` to wire a
/// consumer Shard; entry channels are fed by the controller so there is no
/// `.from(...)`.
pub struct EntryChannel<'b, T> {
    builder: &'b mut ChannelBuilder,
    channel_id: u32,
    name: &'static str,
    _phantom: PhantomData<fn() -> T>,
}

impl<'b, T: 'static + Send + Sync> EntryChannel<'b, T> {
    /// Wire the entry-shard consumer. Compile-time bound: `S::Input == T`.
    pub fn into<S: Shard<Input = T>>(self) -> Channel<T> {
        let shard_id = self.builder.register_shard::<S>();
        let entry = &mut self.builder.channels[self.channel_id as usize];
        entry.consumers.push(shard_id);

        let shard = &mut self.builder.shards[shard_id as usize];
        shard.input_channel_id = Some(self.channel_id);

        Channel::new(self.channel_id)
    }

    /// Override the per-kind default `OverflowPolicy`. Entry channels
    /// default to `DropNewest`; declare `Block` here only if you accept
    /// stalling the shared wire reader on this Relay's saturation (see
    /// `docs/relay_architecture.md` for the co-tenancy hazard).
    pub fn on_full(self, policy: OverflowPolicy) -> Self {
        self.builder.channels[self.channel_id as usize].overflow_policy = policy;
        self
    }

    /// Discarded handle accessor (rarely useful — kept for symmetry).
    pub fn name(&self) -> &'static str {
        self.name
    }
}

/// Inter-shard / plugin-bound channel awaiting a producer.
pub struct ChannelDecl<'b, T> {
    builder: &'b mut ChannelBuilder,
    channel_id: u32,
    name: &'static str,
    _phantom: PhantomData<fn() -> T>,
}

impl<'b, T: 'static + Send + Sync> ChannelDecl<'b, T> {
    /// Override the per-kind default `OverflowPolicy` before wiring the
    /// producer. Inter-shard channels default to `Block`; switch to
    /// `DropNewest` / `DropOldest` on a per-channel basis when ordering
    /// can be sacrificed for liveness.
    pub fn on_full(self, policy: OverflowPolicy) -> Self {
        self.builder.channels[self.channel_id as usize].overflow_policy = policy;
        self
    }

    /// Wire the producer Shard. Compile-time bound: `S::Output == T`.
    /// Multi-producer is a compile error: this consumes `self` and returns
    /// `ChannelDeclWithProducer`, which has no `.from` method.
    pub fn from<S: Shard<Output = T>>(self) -> ChannelDeclWithProducer<'b, T> {
        let shard_id = self.builder.register_shard::<S>();

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
            entry.producer = Some(shard_id);
        }
        let shard = &mut self.builder.shards[shard_id as usize];
        shard.output_channel_id = Some(self.channel_id);

        ChannelDeclWithProducer {
            builder: self.builder,
            channel_id: self.channel_id,
            name: self.name,
            _phantom: PhantomData,
        }
    }
}

/// Channel with a producer registered. Accepts zero or more `.into::<X>()`
/// inter-shard consumers, or one `.to_plugin(...)` plugin destination.
pub struct ChannelDeclWithProducer<'b, T> {
    builder: &'b mut ChannelBuilder,
    channel_id: u32,
    name: &'static str,
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

    /// Wire an inter-shard consumer. Compile-time bound: `S::Input == T`.
    /// Calling `.into` repeatedly is allowed (fan-out).
    pub fn into<S: Shard<Input = T>>(self) -> Self {
        let shard_id = self.builder.register_shard::<S>();
        let entry = &mut self.builder.channels[self.channel_id as usize];
        entry.consumers.push(shard_id);
        let shard = &mut self.builder.shards[shard_id as usize];
        shard.input_channel_id = Some(self.channel_id);
        self
    }

    /// Mark this channel as plugin-bound. The controller's plugin layer
    /// receives the values. v1 requires `T: AsRef<[u8]>` (`Vec<u8>` /
    /// `String`); the producer Shard is responsible for serializing
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
