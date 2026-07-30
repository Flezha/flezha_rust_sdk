//! Channel runtime — typed end-to-end with per-operative namespacing.
//!
//! Hot-path uses interned `u32` channel IDs from a `OperativeTopology`; the
//! `Channel<T>` handle is phantom-typed so wiring errors fire at compile
//! time. Channel names are de-duplicated and assigned sequential IDs at
//! `define_unbounded::<R>()` time and frozen with the topology.

use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

pub mod builder;

pub use builder::{ChannelBuilder, EntryStream, Merge, MergeSource, Stream};

pub type ChannelId = u32;
pub type OperationId = u32;

/// A merge subscription's mapping from a source channel's payload into the
/// consuming operation's `Input` sum type (`multi-input-operations`). Erased
/// from the `Fn(T) -> I` the author wrote; the caller guarantees the value is
/// a `T` because the edge was declared against that channel's payload type.
///
/// The mapping MUST be pure — it runs on live delivery and again on WAL
/// replay, and the two must agree — the same contract entry parsers and
/// output serializers carry.
pub type MergeMapFn = Arc<dyn Fn(&dyn Any) -> Arc<dyn Any + Send + Sync> + Send + Sync + 'static>;

/// One consumer subscription on a channel. A plain `.into::<Op>()` carries no
/// mapping (the channel's payload IS the consumer's `Input`); a
/// `merge::<I>().from(stream, map)` subscription carries the mapping that
/// lifts this channel's payload into the merged `Input`.
#[derive(Clone)]
pub struct ConsumerEdge {
    pub operation: OperationId,
    pub map: Option<MergeMapFn>,
}

impl ConsumerEdge {
    /// Apply this edge's mapping, or `None` when the payload is already the
    /// consumer's input type (the single-input case pays nothing).
    pub(crate) fn map_value(
        &self,
        value: &Arc<dyn Any + Send + Sync>,
    ) -> Option<Arc<dyn Any + Send + Sync>> {
        self.map.as_ref().map(|f| f(value.as_ref()))
    }
}

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

/// Per-channel retry policy applied by the consumer-side dispatcher when an
/// operation invocation fails (returned error or panic). After
/// `max_attempts` total attempts — or immediately on an error the author
/// classified `OperationError::NonRetryable` — the payload is parked to the
/// controller's dead-letter queue and the segment advances. Declared via
/// `.retry_max(...)` / `.retry_backoff_ms(...)` on the channel builder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts (first run included). Default 3.
    pub max_attempts: u32,
    /// Exponential backoff base in milliseconds: attempt n sleeps
    /// `base * 2^(n-1)` (±50% jitter), capped at [`RETRY_BACKOFF_MAX_MS`].
    pub backoff_base_ms: u64,
}

/// Cap on the per-attempt retry backoff, jitter included.
pub const RETRY_BACKOFF_MAX_MS: u64 = 5_000;

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff_base_ms: 50,
        }
    }
}

/// Hybrid plugin sink: the runtime form of `also_output_with(...)`.
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
    /// Consumer subscriptions, each carrying its optional merge mapping.
    pub consumers: Vec<ConsumerEdge>,
    pub plugin_name: Option<Arc<str>>,
    /// For entry channels: convert wire bytes into the typed payload
    /// (`Arc<S::Input>`). For inter-operation channels: `None` — payloads
    /// arrive already typed via `ctx.emit`. For plugin-bound channels:
    /// `None` — handled by the producer-side serializer.
    pub wrap_input: Option<EntryWrapFn>,
    /// Entry channels whose `wrap_input` is a real parser (typed JSON or a
    /// custom `input_with` parser): a failing decode rides this channel's
    /// retry→DLQ ladder instead of the passthrough drop-and-log path.
    pub entry_parsed: bool,
    /// For pure `PluginBound` channels: serialize the typed payload to
    /// wire bytes. For other kinds: `None`. Hybrid channels populate
    /// `plugin_sink` instead.
    pub plugin_serializer: Option<PluginSerializeFn>,
    /// For hybrid `InterOperation` channels declared with
    /// `also_output_with(...)` / `also_output(...)`: the named plugin sink. Mutually
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
    /// Retry-then-park policy applied by this channel's consumer dispatchers.
    /// Plugin-bound channels have no SDK-side consumer and ignore this value.
    pub retry: RetryPolicy,
    /// Symmetric byte codec for inter-operation payloads, declared via
    /// `.codec()` / `.codec_with(...)`. Required on every inter-op channel of
    /// a durable structured bounded class so the stage handoff batch can be
    /// journaled at the op-commit barrier and re-seeded on crash-resume
    /// (`bounded-op-journal`). `None` elsewhere.
    pub codec: Option<ChannelCodec>,
}

impl ChannelDescriptor {
    /// The consuming operations, mappings stripped — what the graph-shape
    /// walks and the bounded DAG builder need.
    pub fn consumer_ids(&self) -> impl Iterator<Item = OperationId> + '_ {
        self.consumers.iter().map(|e| e.operation)
    }
}

/// Typed→bytes / bytes→typed codec pair for an inter-operation channel's
/// payload. `encode` receives the type-erased collected value; `decode` is
/// the inter-op sibling of the entry channel's `wrap_input`.
#[derive(Clone)]
pub struct ChannelCodec {
    pub encode: CodecEncodeFn,
    pub decode: EntryWrapFn,
}

pub type CodecEncodeFn = Arc<dyn Fn(&dyn Any) -> Result<Vec<u8>, String> + Send + Sync + 'static>;

pub type EntryWrapFn =
    Arc<dyn Fn(Vec<u8>) -> Result<Arc<dyn Any + Send + Sync>, String> + Send + Sync + 'static>;

pub type PluginSerializeFn = Arc<dyn Fn(&dyn Any) -> Vec<u8> + Send + Sync + 'static>;
