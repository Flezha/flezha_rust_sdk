//! Channel runtime — typed end-to-end with per-relay namespacing.
//!
//! Hot-path uses interned `u32` channel IDs from a `RelayTopology`; the
//! `Channel<T>` handle is phantom-typed so wiring errors fire at compile
//! time. Channel names are de-duplicated and assigned sequential IDs at
//! `register_relay::<R>()` time and frozen for the process lifetime.

use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

pub mod builder;
pub mod entry_type;

pub use builder::{ChannelBuilder, ChannelDecl, ChannelDeclWithProducer, EntryChannel};
pub use entry_type::EntryChannelType;

pub type ChannelId = u32;
pub type ShardId = u32;

/// Phantom-typed channel handle. The `id` is the index into
/// `RelayTopology::channels`; the `T` parameter exists only for
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
        Self {
            id: self.id,
            _phantom: PhantomData,
        }
    }
}

impl<T> Copy for Channel<T> {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelKind {
    Entry,
    InterShard,
    PluginBound,
}

/// Per-channel backpressure policy applied at the producer-side push
/// site when the consumer's bounded mailbox is full. Declared via
/// `.on_full(...)` on the channel builder; defaults to `DropNewest` for
/// entry channels and `Block` for inter-shard channels. Plugin-bound
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

pub struct ChannelDescriptor {
    pub name: Arc<str>,
    pub kind: ChannelKind,
    pub producer: Option<ShardId>,
    pub consumers: Vec<ShardId>,
    pub plugin_name: Option<Arc<str>>,
    /// For entry channels: convert wire bytes into the typed payload
    /// (`Arc<S::Input>`). For inter-shard channels: `None` — payloads
    /// arrive already typed via `ctx.emit`. For plugin-bound channels:
    /// `None` — handled by the producer-side serializer.
    pub wrap_input: Option<EntryWrapFn>,
    /// For plugin-bound channels: serialize the typed payload to wire
    /// bytes. For other kinds: `None`.
    pub plugin_serializer: Option<PluginSerializeFn>,
    /// Backpressure policy applied at the push site when the consumer's
    /// mailbox is full. Set at declaration time per channel kind, optionally
    /// overridden by `.on_full(...)`. Plugin-bound channels carry a
    /// placeholder value that is never consulted.
    pub overflow_policy: OverflowPolicy,
}

pub type EntryWrapFn =
    Arc<dyn Fn(Vec<u8>) -> Result<Arc<dyn Any + Send + Sync>, String> + Send + Sync + 'static>;

pub type PluginSerializeFn =
    Arc<dyn Fn(&dyn Any) -> Vec<u8> + Send + Sync + 'static>;
