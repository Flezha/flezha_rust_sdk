use std::any::Any;
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;

use crate::operation::error::OperationError;

/// Reserved metadata namespace: runtime identity stamps (`flz.segment`,
/// `flz.seq`, `flz.key`) live here; user-supplied keys with this prefix
/// are rejected at emit time so stamps can never be spoofed or clobbered.
pub const RESERVED_METADATA_PREFIX: &str = "flz.";

/// Per-invocation context for a Operation's `run` method. Borrows the runner's
/// per-segment routing tables. Not Send/Sync — lives only across one
/// `Operation::run` call.
///
/// The dispatcher stamps `output_channel_id` (cached at operative-build time)
/// so emit incurs zero channel-name lookups on the hot path. It also
/// evaluates the Operation's declared `Clock` once at dispatch entry —
/// [`Self::now`] serves that single value for the whole invocation — and
/// carries the replay-stable inputs [`Self::stable_id`] derives from.
pub struct OperationCtx<'a> {
    pub(crate) emitter: &'a mut dyn EmitSink,
    now: u64,
    invocation_seq: u64,
    segment_identity: &'a [u8],
    stable_id_ordinal: Cell<u32>,
    checkpoint: bool,
}

/// Replay-stable 128-bit identifier returned by [`OperationCtx::stable_id`].
/// Derived by a frozen FNV-1a-128 over
/// `segment identity ‖ change seq ‖ per-invocation call ordinal` — the
/// algorithm is fixed by spec, independent of any library version.
/// `Display` renders 32 lowercase hex chars. The drop-in replacement for
/// `Uuid::new_v4()` in operative code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StableId(pub u128);

impl std::fmt::Display for StableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

impl StableId {
    pub(crate) fn derive(segment_identity: &[u8], seq: u64, ordinal: u32) -> Self {
        let mut hash = fnv1a128(FNV128_OFFSET_BASIS, segment_identity);
        hash = fnv1a128(hash, &seq.to_be_bytes());
        hash = fnv1a128(hash, &ordinal.to_be_bytes());
        StableId(hash)
    }
}

const FNV128_OFFSET_BASIS: u128 = 0x6c62272e07bb014262b821756295c58d;
const FNV128_PRIME: u128 = 0x0000000001000000000000000000013b;

/// FNV-1a-128 (frozen by spec: xor byte, multiply by the FNV 128 prime).
fn fnv1a128(mut hash: u128, bytes: &[u8]) -> u128 {
    for &b in bytes {
        hash ^= b as u128;
        hash = hash.wrapping_mul(FNV128_PRIME);
    }
    hash
}

/// Internal trait the runtime implements per-segment. Lets OperationCtx stay
/// non-generic while the underlying dispatcher carries the typed wiring.
pub(crate) trait EmitSink {
    fn emit_inter_operation(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
    ) -> Result<(), OperationError>;

    fn emit_plugin(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), OperationError>;

    /// Hybrid emit: fan out to inter-operation consumers and (if a plugin
    /// sink is configured) serialize the value once and forward bytes
    /// to the plugin wire with the supplied metadata + natural key.
    /// Default implementation routes through `emit_inter_operation` only —
    /// sinks without a plugin attachment can ignore `meta` / `key`.
    fn emit_inter_operation_with_meta(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        _meta: Option<HashMap<String, String>>,
        _key: Option<String>,
    ) -> Result<(), OperationError> {
        self.emit_inter_operation(value)
    }

    fn channel_name(&self) -> &str;

    fn is_plugin_bound(&self) -> bool;

    /// True iff the channel is `InterOperation` with a `plugin_sink`
    /// attached via `also_output_with(...)`. Hybrid emits route
    /// through `emit_inter_operation{,_with_meta}` rather than `emit_plugin`.
    fn has_plugin_sink(&self) -> bool {
        false
    }

    /// True iff the output channel is declared `.keyed()` — every plugin
    /// emit must then carry a natural key via `emit_keyed`.
    fn is_keyed(&self) -> bool {
        false
    }
}

impl<'a> OperationCtx<'a> {
    pub(crate) fn new(
        emitter: &'a mut dyn EmitSink,
        now: u64,
        invocation_seq: u64,
        segment_identity: &'a [u8],
    ) -> Self {
        Self {
            emitter,
            now,
            invocation_seq,
            segment_identity,
            stable_id_ordinal: Cell::new(0),
            checkpoint: false,
        }
    }

    /// Close the current checkpoint interval: every input folded in since the
    /// last checkpoint — this one included — is acknowledged, and the state
    /// they accumulated is made durable, as one atomic record.
    ///
    /// Only meaningful under `ChannelBuilder::checkpoint_ack()`. An operative
    /// on the default per-input granularity already acknowledges every input,
    /// so the call is a no-op there rather than an error — a checkpointing
    /// Operation stays correct if its operative later drops the declaration.
    ///
    /// Call it where the accumulated state is genuinely coherent (a closed
    /// window, a completed batch): the interval you leave open is the work a
    /// crash replays and the input log retains.
    pub fn checkpoint(&mut self) {
        self.checkpoint = true;
    }

    /// Whether `run` closed the interval with [`Self::checkpoint`].
    pub(crate) fn checkpointed(&self) -> bool {
        self.checkpoint
    }

    /// The invocation's time value, computed once by the runtime from the
    /// Operation's declared `Clock` before `run` executes: epoch
    /// milliseconds for `WallClock`, the extracted input timestamp for
    /// `EventTimeFrom`, the durable change seq for `LogicalClock`. Every
    /// call within one invocation returns the same value. This is the
    /// determinism-contract replacement for `SystemTime::now()`.
    pub fn now(&self) -> u64 {
        self.now
    }

    /// A deterministic, replay-stable 128-bit identifier: derived from the
    /// Segment identity, the invocation's durable change seq, and a
    /// per-invocation call ordinal. Repeated calls in one invocation
    /// differ; the n-th call in a replayed invocation equals the n-th
    /// call in the original. The determinism-contract replacement for
    /// `Uuid::new_v4()`.
    pub fn stable_id(&self) -> StableId {
        let ordinal = self.stable_id_ordinal.get();
        self.stable_id_ordinal.set(ordinal + 1);
        StableId::derive(self.segment_identity, self.invocation_seq, ordinal)
    }

    /// Emit a typed value on the producer-edge channel for this Operation.
    /// `C` must match `Operation::Output`; the channel id was cached at
    /// operative-build time from the producer's output-stream wiring
    /// (`.into::<S>()`), so this call performs no name lookups. On a `.keyed()`
    /// channel this fails with `KeyRequiredOnKeyedChannel` — use
    /// [`Self::emit_keyed`].
    pub fn emit<C>(&mut self, value: C) -> Result<(), OperationError>
    where
        C: 'static + Send + Sync,
    {
        self.route(Arc::new(value), None, None)
    }

    /// Emit with user-attached string-keyed metadata. Valid on
    /// plugin-bound channels and on hybrid (`InterOperation` +
    /// `also_output_with`) channels. Pure inter-operation channels with
    /// no plugin sink return `MetadataNotAllowedOnInterOperationChannel`.
    /// Keys in the reserved `flz.` namespace are rejected.
    pub fn emit_with_meta<C>(
        &mut self,
        value: C,
        meta: HashMap<String, String>,
    ) -> Result<(), OperationError>
    where
        C: 'static + Send + Sync,
    {
        self.route(Arc::new(value), Some(meta), None)
    }

    /// Emit on a `.keyed()` channel with the output's natural key. The
    /// runtime stamps `flz.segment` / `flz.seq` / `flz.key` on the plugin
    /// frame; the key must be replay-stable by the determinism contract
    /// (derived from input/state/ctx, never from ambient sources). Fails
    /// with `KeyNotAllowedOnNonKeyedChannel` on channels not declared
    /// `.keyed()`.
    pub fn emit_keyed<C>(&mut self, key: impl Into<String>, value: C) -> Result<(), OperationError>
    where
        C: 'static + Send + Sync,
    {
        self.route(Arc::new(value), None, Some(key.into()))
    }

    /// [`Self::emit_keyed`] with user metadata. Identity stamps are
    /// appended after user metadata so they can never be clobbered; keys
    /// in the reserved `flz.` namespace are rejected.
    pub fn emit_keyed_with_meta<C>(
        &mut self,
        key: impl Into<String>,
        value: C,
        meta: HashMap<String, String>,
    ) -> Result<(), OperationError>
    where
        C: 'static + Send + Sync,
    {
        self.route(Arc::new(value), Some(meta), Some(key.into()))
    }

    /// Single emit funnel: enforces the keyed-channel contract (the
    /// channel declaration is the single source of truth for whether a
    /// key is required), rejects reserved `flz.` metadata keys, then
    /// routes by channel shape.
    fn route(
        &mut self,
        arc: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), OperationError> {
        match (self.emitter.is_keyed(), key.is_some()) {
            (true, false) => {
                return Err(OperationError::KeyRequiredOnKeyedChannel {
                    channel: self.emitter.channel_name().to_string(),
                })
            }
            (false, true) => {
                return Err(OperationError::KeyNotAllowedOnNonKeyedChannel {
                    channel: self.emitter.channel_name().to_string(),
                })
            }
            _ => {}
        }
        if let Some(meta) = &meta {
            if let Some(reserved) = meta
                .keys()
                .find(|k| k.starts_with(RESERVED_METADATA_PREFIX))
            {
                return Err(OperationError::ReservedMetadataKey {
                    key: reserved.clone(),
                });
            }
        }
        let plugin_bound = self.emitter.is_plugin_bound();
        if meta.is_some() && !plugin_bound && !self.emitter.has_plugin_sink() {
            return Err(OperationError::MetadataNotAllowedOnInterOperationChannel {
                channel: self.emitter.channel_name().to_string(),
            });
        }
        if plugin_bound {
            self.emitter.emit_plugin(arc, meta, key)
        } else if meta.is_some() || key.is_some() {
            self.emitter.emit_inter_operation_with_meta(arc, meta, key)
        } else {
            self.emitter.emit_inter_operation(arc)
        }
    }
}
