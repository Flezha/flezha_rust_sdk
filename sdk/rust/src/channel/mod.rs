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

pub use builder::{
    ChannelBuilder, EntryStream, EntryTarget, Merge, MergeSource, NodeTarget, Stream, StreamTarget,
};

pub type ChannelId = u32;
pub type ProcedureId = u32;

/// What one consumer edge's transform yields for one input value: nothing (a
/// `filter` rejection), the common single reshaped value, or a fan-out
/// (`flat_map`). `Drop` and `One` allocate nothing beyond the value itself;
/// only the fan-out pays a `Vec`.
pub enum EdgeYield {
    Drop,
    One(Arc<dyn Any + Send + Sync>),
    Many(Vec<Arc<dyn Any + Send + Sync>>),
}

impl IntoIterator for EdgeYield {
    type Item = Arc<dyn Any + Send + Sync>;
    type IntoIter = EdgeYieldIter;

    fn into_iter(self) -> EdgeYieldIter {
        EdgeYieldIter(match self {
            EdgeYield::Drop => YieldState::Done,
            EdgeYield::One(v) => YieldState::One(v),
            EdgeYield::Many(vs) => YieldState::Many(vs.into_iter()),
        })
    }
}

/// The values one [`EdgeYield`] delivers. Allocation-free for `Drop` and
/// `One` — only the fan-out already owns a `Vec`.
pub struct EdgeYieldIter(YieldState);

enum YieldState {
    Done,
    One(Arc<dyn Any + Send + Sync>),
    Many(std::vec::IntoIter<Arc<dyn Any + Send + Sync>>),
}

impl Iterator for EdgeYieldIter {
    type Item = Arc<dyn Any + Send + Sync>;

    fn next(&mut self) -> Option<Self::Item> {
        match std::mem::replace(&mut self.0, YieldState::Done) {
            YieldState::Done => None,
            YieldState::One(v) => Some(v),
            YieldState::Many(mut rest) => {
                let next = rest.next();
                self.0 = YieldState::Many(rest);
                next
            }
        }
    }
}

/// One consumer edge's value transform: every `map` / `filter` / `flat_map`
/// and every merge mapping declared before the closing `.into::<Proc>()`,
/// composed into a single closure. Erased from the typed closures the author
/// wrote; the caller guarantees the value's type because the chain was
/// declared against that channel's payload type.
///
/// The transform MUST be pure — it runs on live delivery and again on WAL
/// replay, and the two must agree — the same contract entry parsers and
/// output serializers carry.
/// Takes the value behind its `Arc` rather than a bare `&dyn Any` so a
/// surviving `filter` yields `One(Arc::clone(..))` — a refcount bump, not a
/// payload clone.
pub type EdgeTransformFn =
    Arc<dyn Fn(&Arc<dyn Any + Send + Sync>) -> EdgeYield + Send + Sync + 'static>;

/// One consumer edge's Entity key function, declared with `.key_by(f)` and
/// evaluated on the **post-transform** value. Total by signature: it names an
/// Entity, it never declines — discarding an input is `filter`'s job. Carries
/// the same purity contract as [`EdgeTransformFn`].
pub type EdgeKeyFn = Arc<dyn Fn(&dyn Any) -> String + Send + Sync + 'static>;

/// One consumer subscription on a channel. A plain `.into::<Proc>()` carries
/// neither closure (the channel's payload IS the consumer's `Input`, and the
/// consumer's Entity mode is its own); a chain of transforms, a `.key_by(f)`,
/// or a `merge::<I>().from(stream, map)` subscription stores them here.
#[derive(Clone)]
pub struct ConsumerEdge {
    pub procedure: ProcedureId,
    pub transform: Option<EdgeTransformFn>,
    pub key: Option<EdgeKeyFn>,
}

impl ConsumerEdge {
    /// Apply this edge's transform, or `Ok(None)` when the payload already IS
    /// the consumer's input type (the transform-free case pays nothing).
    ///
    /// Contained: the closure is author code evaluated on the routing thread
    /// (and, on a bounded chain, on the driver thread), so an unwind becomes an
    /// error the caller's ladder handles rather than a dead thread.
    pub(crate) fn transform_value(
        &self,
        value: &Arc<dyn Any + Send + Sync>,
    ) -> Result<Option<EdgeYield>, String> {
        let Some(f) = self.transform.as_ref() else {
            return Ok(None);
        };
        contain("transform", || f(value)).map(Some)
    }

    /// Evaluate this edge's key function on the post-transform value, or
    /// `Ok(None)` when the edge declares none. Contained like the transform.
    pub(crate) fn key_of(&self, value: &dyn Any) -> Result<Option<String>, String> {
        let Some(f) = self.key.as_ref() else {
            return Ok(None);
        };
        contain("key_by", || f(value)).map(Some)
    }
}

/// Run one edge closure, turning an unwind into the error its caller's
/// retry→DLQ ladder (unbounded) or abort-and-compensate path (bounded) reads.
fn contain<R>(what: &str, f: impl FnOnce() -> R) -> Result<R, String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).map_err(|panic| {
        format!(
            "edge {what} panicked: {}",
            crate::procedure::error::format_panic(panic)
        )
    })
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
    InterProcedure,
    PluginBound,
}

/// Per-channel backpressure policy applied at the producer-side push
/// site when the consumer's bounded mailbox is full. Declared via
/// `.on_full(...)` on the channel builder; defaults to `DropNewest` for
/// entry channels and `Block` for inter-procedure channels. Plugin-bound
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
/// procedure invocation fails (returned error or panic). After
/// `max_attempts` total attempts — or immediately on an error the author
/// classified `ProcedureError::NonRetryable` — the payload is parked to the
/// controller's dead-letter queue and the entity advances. Declared via
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
/// Lives alongside an `InterProcedure` channel's consumer fan-out so a
/// single emit reaches both consumer mailboxes (typed) and the named
/// plugin (serialized to bytes).
pub struct PluginSink {
    pub name: Arc<str>,
    pub serializer: PluginSerializeFn,
}

pub struct ChannelDescriptor {
    pub name: Arc<str>,
    pub kind: ChannelKind,
    pub producer: Option<ProcedureId>,
    /// Consumer subscriptions, each carrying its optional transform and key.
    pub consumers: Vec<ConsumerEdge>,
    pub plugin_name: Option<Arc<str>>,
    /// For entry channels: convert wire bytes into the typed payload
    /// (`Arc<S::Input>`). For inter-procedure channels: `None` — payloads
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
    /// For hybrid `InterProcedure` channels declared with
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
    /// Symmetric byte codec for inter-procedure payloads, declared via
    /// `.codec()` / `.codec_with(...)`. Required on every inter-proc channel of
    /// a durable structured bounded class so the stage handoff batch can be
    /// journaled at the proc-commit barrier and re-seeded on crash-resume
    /// (`bounded-proc-journal`). `None` elsewhere.
    pub codec: Option<ChannelCodec>,
}

impl ChannelDescriptor {
    /// The consuming procedures, edge closures stripped — what the graph-shape
    /// walks and the bounded DAG builder need.
    pub fn consumer_ids(&self) -> impl Iterator<Item = ProcedureId> + '_ {
        self.consumers.iter().map(|e| e.procedure)
    }
}

/// Typed→bytes / bytes→typed codec pair for an inter-procedure channel's
/// payload. `encode` receives the type-erased collected value; `decode` is
/// the inter-proc sibling of the entry channel's `wrap_input`.
#[derive(Clone)]
pub struct ChannelCodec {
    pub encode: CodecEncodeFn,
    pub decode: EntryWrapFn,
}

pub type CodecEncodeFn = Arc<dyn Fn(&dyn Any) -> Result<Vec<u8>, String> + Send + Sync + 'static>;

pub type EntryWrapFn =
    Arc<dyn Fn(Vec<u8>) -> Result<Arc<dyn Any + Send + Sync>, String> + Send + Sync + 'static>;

pub type PluginSerializeFn = Arc<dyn Fn(&dyn Any) -> Vec<u8> + Send + Sync + 'static>;
