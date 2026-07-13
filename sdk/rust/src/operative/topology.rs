//! `OperativeTopology`: shared, immutable topology produced by
//! `register_unbounded::<R>()`. Section 2 implements `build_topology`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::channel::builder::{
    DeserializeStateFn, OperationKeyFn, SegmentBuilder, SegmentForFn, SerializeStateFn,
};
use crate::channel::{ChannelDescriptor, ChannelId, OperationId};

/// How a Operation maps inputs to Segments. Resolved once at registration
/// from `(operation_keys().is_empty(), OPERATION_KEY_OVERRIDDEN)`:
/// `(true, false)` → `Single`, `(false, _)` → `Static`,
/// `(true, true)` → `Dynamic`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentingMode {
    /// One Segment per Operation; `operation_key()` / `operation_keys()` unused.
    Single,
    /// Fixed keyset declared by `operation_keys()`; routed by `segment_for`.
    Static,
    /// Unbounded keyset; Segments created lazily by `operation_key()`.
    Dynamic,
}

/// One Operation's slot in the topology. Pre-resolved channel ids so the
/// hot path needs zero name lookups.
pub struct OperationDescriptor {
    pub name: Arc<str>,
    pub segment_keys: Vec<&'static str>,
    pub segment_for_overridden: bool,
    pub operation_key_overridden: bool,
    /// `Operation::COMPENSATE_OVERRIDDEN`, captured at registration. The bounded
    /// saga driver fires compensation only for operations with this set; a
    /// unbounded operative never reads it.
    pub compensate_overridden: bool,
    /// Segmenting mode resolved at registration. Drives whether the
    /// dispatch path routes by static key, single slot, or the dynamic
    /// registry.
    pub mode: SegmentingMode,
    /// Dynamic-mode idle-TTL eviction window (`Operation::segment_idle_ttl`).
    /// `None` (the default) means no eviction; meaningful only when
    /// `mode == Dynamic`.
    pub idle_ttl: Option<Duration>,
    /// Dynamic-mode soft cap on live Segments (`Operation::max_segments`).
    pub max_segments: usize,
    /// Type-erased `Operation::operation_key`. `Some` only for dynamic-mode
    /// Operations; the dispatch path calls it to derive the segment key.
    pub operation_key_erased: Option<OperationKeyFn>,
    pub input_channel_id: Option<ChannelId>,
    pub output_channel_id: Option<ChannelId>,
    pub input_type_id: std::any::TypeId,
    pub output_type_id: std::any::TypeId,
    /// Final per-Segment mailbox capacity for every Segment this
    /// Operation owns, after applying the override / env-var / default
    /// precedence in `operative::builder::resolve_mailbox_capacity`.
    pub mailbox_capacity: usize,
    /// `Operation::MAILBOX_CAPACITY_OVERRIDDEN` captured at registration time.
    /// Carried for diagnostics so a future tooling change can show which
    /// Operations opted into an explicit capacity.
    pub mailbox_capacity_overridden: bool,
    pub build_segment: SegmentBuilder,
    pub segment_for_erased: SegmentForFn,
    /// JSON-serialize a Segment's typed state via the per-Operation fn
    /// pointer captured at registration time. Used by `SegmentState`
    /// and `MigrationSerialization` to produce the Decision-10 wire blob.
    pub serialize_state: SerializeStateFn,
    /// Deserialize a per-Operation JSON value into the type-erased state
    /// payload that `SegmentSlot::state` holds. Used by
    /// `apply_init_values` (and `MigrationDeserialization`) to seed
    /// Segments from the controller-supplied JSON blob.
    pub deserialize_state: DeserializeStateFn,
}

pub struct OperativeTopology {
    pub channels: Vec<ChannelDescriptor>,
    pub name_to_id: HashMap<&'static str, ChannelId>,
    pub operations: Vec<OperationDescriptor>,
    pub operation_name_to_id: HashMap<&'static str, OperationId>,
    pub factory_id: &'static str,
    /// Captured `UnboundedOperative::PERSISTENCE_ENABLED` const at registration.
    /// `UnboundedInstance::new` resolves the final per-instance flag using
    /// this const + the executor override from operative-start metadata.
    pub persistence_enabled_const: bool,
    /// Effectively-once delivery tier, declared via
    /// `ChannelBuilder::effectively_once()`. Frozen at registration;
    /// announced to the controller in the `AddOperative` response metadata
    /// and drives the tier emit path (emits ride the reliable state sink
    /// inside the atomic effect record instead of the primary stream).
    pub effectively_once: bool,

    /// `ChannelBuilder::correction_window()`. `None` (the default) means the
    /// operative never corrects: a late input is just an input. Declared, it
    /// announces the window to the controller in the `AddOperative` response
    /// metadata and arms lateness detection on the dispatch path.
    pub correction: Option<CorrectionPolicy>,

    /// Where the effectively-once tier acknowledges its inputs, declared via
    /// `ChannelBuilder::checkpoint_ack()`. Frozen at registration; announced
    /// to the controller in the `AddOperative` response metadata.
    pub ack_granularity: AckGranularity,
}

/// Where a tier-enabled operative's durable consumed-offset advances.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AckGranularity {
    /// One atomic record per effectful invocation, each acknowledging its own
    /// input. The default, and the only granularity for an operative whose
    /// state is kernel-visible after every input.
    #[default]
    PerInput,
    /// The consumed-offset advances only at an explicit `ctx.checkpoint()`.
    /// Changes applied in between are held in the Segment's flush buffer and
    /// ship — with the offset of the input that closed the interval — as one
    /// fsynced record, so the state a restore sees and the offset it resumes
    /// from can never tear. The interval's inputs are re-forwarded from the
    /// input log on recovery and its outputs re-emitted; both are absorbed by
    /// the keyed-sink dedup the tier already requires.
    Checkpoint,
}

/// What an operative does about an input that arrives behind its Segment's
/// event-time high-water. Frozen at registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorrectionPolicy {
    /// How far behind the high-water an input may be and still be repaired by
    /// a rewind-restore-replay. Bounds the WAL rewind, the input-log slice,
    /// and hence the retention the controller must guarantee.
    pub window_ms: u64,
    /// What happens to an input that is later than `window_ms` — the past is
    /// too old to rebuild.
    pub beyond_window: BeyondWindowPolicy,
}

/// Fate of a late input the correction window cannot reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BeyondWindowPolicy {
    /// Process it as if it had just happened — the engine's behaviour with no
    /// correction declared at all, so declaring a window never silently starts
    /// discarding data that used to be processed. The default for that reason.
    #[default]
    ProcessAsArrival,
    /// Discard it. Opt in only when a stale event is worse than a missing one.
    Drop,
}

impl OperativeTopology {
    pub fn lookup_channel_id(&self, name: &str) -> Option<ChannelId> {
        self.name_to_id.get(name).copied()
    }

    pub fn channel(&self, id: ChannelId) -> Option<&ChannelDescriptor> {
        self.channels.get(id as usize)
    }

    pub fn operation(&self, id: OperationId) -> Option<&OperationDescriptor> {
        self.operations.get(id as usize)
    }
}
