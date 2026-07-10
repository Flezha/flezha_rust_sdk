//! Channel runtime — typed end-to-end with per-operative namespacing.
//!
//! Hot-path uses interned `u32` channel IDs from a `OperativeTopology`; the
//! `Channel<T>` handle is phantom-typed so wiring errors fire at compile
//! time. Channel names are de-duplicated and assigned sequential IDs at
//! `register_unbounded::<R>()` time and frozen for the process lifetime.

use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

pub mod builder;
pub mod entry_type;

pub use builder::{ChannelBuilder, ChannelDecl, ChannelDeclWithProducer, EntryChannel};
pub use entry_type::EntryChannelType;

pub type ChannelId = u32;
pub type OperationId = u32;

/// Phantom-typed channel handle. The `id` is the index into
/// `OperativeTopology::channels`; the `T` parameter exists only for
/// compile-time wiring checks via the builder methods.
pub struct Channel<T> {
    pub id: ChannelId,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Channel<T> {
    pub(crate) fn new(id: ChannelId) -> Self {
        Self {
            id,
            _phantom: PhantomData,
        }
    }
}

impl<T> Clone for Channel<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Channel<T> {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelKind {
    Entry,
    InterOperation,
    PluginBound,
}

/// Per-channel backpressure policy applied at the producer-side push
/// site when the consumer's bounded mailbox is full. Declared via
/// `.on_full(...)` on the channel builder; defaults to `DropNewest` for
/// entry channels and `Block` for inter-operation channels. Plugin-bound
/// channels have no SDK-side mailbox and ignore this value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Producer parks until the consumer drains an item, then retries.
    /// Preserves ordering at the cost of producer-thread stall.
    Block,
    /// Incoming item is discarded; consumer's `drops_newest_total` increments.
    DropNewest,
    /// Consumer's mailbox atomically drops its oldest item and accepts the
    /// new one; consumer's `drops_oldest_total` increments. Best-effort
    /// under MPMC contention.
    DropOldest,
}

/// Hybrid plugin sink: the runtime form of `also_to_plugin_with(...)`.
/// Lives alongside an `InterOperation` channel's consumer fan-out so a
/// single emit reaches both consumer mailboxes (typed) and the named
/// plugin (serialized to bytes).
pub struct PluginSink {
    pub name: Arc<str>,
    pub serializer: PluginSerializeFn,
}

pub struct ChannelDescriptor {
    pub name: Arc<str>,
    pub kind: ChannelKind,
    pub producer: Option<OperationId>,
    pub consumers: Vec<OperationId>,
    pub plugin_name: Option<Arc<str>>,
    /// For entry channels: convert wire bytes into the typed payload
    /// (`Arc<S::Input>`). For inter-operation channels: `None` — payloads
    /// arrive already typed via `ctx.emit`. For plugin-bound channels:
    /// `None` — handled by the producer-side serializer.
    pub wrap_input: Option<EntryWrapFn>,
    /// For pure `PluginBound` channels: serialize the typed payload to
    /// wire bytes. For other kinds: `None`. Hybrid channels populate
    /// `plugin_sink` instead.
    pub plugin_serializer: Option<PluginSerializeFn>,
    /// For hybrid `InterOperation` channels declared with
    /// `also_to_plugin_with(...)`: the named plugin sink. Mutually
    /// exclusive with `plugin_serializer` / `plugin_name`.
    pub plugin_sink: Option<PluginSink>,
    /// Output-identity opt-in (`.keyed()`): plugin emits require a natural
    /// key and carry `flz.*` identity stamps. Always paired with a plugin
    /// wire (validated at build).
    pub keyed: bool,
    /// Backpressure policy applied at the push site when the consumer's
    /// mailbox is full. Set at declaration time per channel kind, optionally
    /// overridden by `.on_full(...)`. Plugin-bound channels carry a
    /// placeholder value that is never consulted.
    pub overflow_policy: OverflowPolicy,
}

pub type EntryWrapFn =
    Arc<dyn Fn(Vec<u8>) -> Result<Arc<dyn Any + Send + Sync>, String> + Send + Sync + 'static>;

pub type PluginSerializeFn = Arc<dyn Fn(&dyn Any) -> Vec<u8> + Send + Sync + 'static>;
