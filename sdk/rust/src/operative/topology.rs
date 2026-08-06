//! `OperativeTopology`: shared, immutable topology produced by
//! `define_unbounded::<R>()`. Section 2 implements `build_topology`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::channel::builder::{
    DeserializeStateFn, EntitiesForFn, EntityBuilder, EntityKeyFn, SerializeStateFn,
};
use crate::channel::{ChannelDescriptor, ChannelId, ProcedureId};

/// How a Procedure maps inputs to Entities. Resolved once at registration
/// from `(entity_keys().is_empty(), ENTITY_KEY_OVERRIDDEN)`:
/// `(true, false)` → `Single`, `(false, _)` → `Static`,
/// `(true, true)` → `Dynamic`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityMode {
    /// One Entity per Procedure; `entity_key()` / `entity_keys()` unused.
    Single,
    /// Fixed keyset declared by `entity_keys()`; routed by `entities_for`.
    Static,
    /// Unbounded keyset; Entities created lazily by `entity_key()`.
    Dynamic,
}

/// One Procedure's slot in the topology. Pre-resolved channel ids so the
/// hot path needs zero name lookups.
pub struct ProcedureDescriptor {
    pub name: Arc<str>,
    pub entity_keys: Vec<&'static str>,
    pub entities_for_overridden: bool,
    pub entity_key_overridden: bool,
    /// `Procedure::COMPENSATE_OVERRIDDEN`, captured at registration. The bounded
    /// saga driver fires compensation only for procedures with this set; a
    /// unbounded operative never reads it.
    pub compensate_overridden: bool,
    /// Entity mode resolved at registration. Drives whether the
    /// dispatch path routes by static key, single slot, or the dynamic
    /// registry.
    pub mode: EntityMode,
    /// Dynamic-mode idle-TTL eviction window (`Procedure::entity_idle_ttl`).
    /// `None` (the default) means no eviction; meaningful only when
    /// `mode == Dynamic`.
    pub idle_ttl: Option<Duration>,
    /// Dynamic-mode soft cap on live Entities (`Procedure::max_entities`).
    pub max_entities: usize,
    /// Type-erased `Procedure::entity_key`. `Some` only for dynamic-mode
    /// Procedures; the dispatch path calls it to derive the entity key.
    pub entity_key_erased: Option<EntityKeyFn>,
    /// Channels this procedure consumes. Several = fan-in (the node receives
    /// the accumulated contributions of all its producers); empty only for an
    /// procedure never wired as a consumer.
    pub input_channel_ids: Vec<ChannelId>,
    pub output_channel_id: Option<ChannelId>,
    pub input_type_id: std::any::TypeId,
    pub output_type_id: std::any::TypeId,
    /// Final per-Entity mailbox capacity for every Entity this
    /// Procedure owns, after applying the override / env-var / default
    /// precedence in `operative::builder::resolve_mailbox_capacity`.
    pub mailbox_capacity: usize,
    /// `Procedure::MAILBOX_CAPACITY_OVERRIDDEN` captured at registration time.
    /// Carried for diagnostics so a future tooling change can show which
    /// Procedures opted into an explicit capacity.
    pub mailbox_capacity_overridden: bool,
    /// `Procedure::compact_after_bytes()` captured at registration time —
    /// the per-Entity WAL size threshold the controller resolves for
    /// compaction, announced in the `AddOperative` response metadata.
    pub compact_after_bytes: usize,
    pub build_entity: EntityBuilder,
    pub entities_for_erased: EntitiesForFn,
    /// JSON-serialize an Entity's typed state via the per-Procedure fn
    /// pointer captured at registration time. Used by `EntityState`
    /// and `MigrationSerialization` to produce the Decision-10 wire blob.
    pub serialize_state: SerializeStateFn,
    /// Deserialize a per-Procedure JSON value into the type-erased state
    /// payload that `EntitySlot::state` holds. Used by
    /// `apply_init_values` (and `MigrationDeserialization`) to seed
    /// Entities from the controller-supplied JSON blob.
    pub deserialize_state: DeserializeStateFn,
}

pub struct OperativeTopology {
    pub channels: Vec<ChannelDescriptor>,
    pub name_to_id: HashMap<&'static str, ChannelId>,
    pub procedures: Vec<ProcedureDescriptor>,
    pub procedure_name_to_id: HashMap<&'static str, ProcedureId>,
    pub factory_id: &'static str,
    /// Captured `UnboundedOperative::PERSISTENCE_ENABLED` const at registration.
    /// `UnboundedInstance::new` resolves the final per-instance flag using
    /// this const + the runner override from operative-start metadata.
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
    /// Changes applied in between are held in the Entity's flush buffer and
    /// ship — with the offset of the input that closed the interval — as one
    /// fsynced record, so the state a restore sees and the offset it resumes
    /// from can never tear. The interval's inputs are re-forwarded from the
    /// input log on recovery and its outputs re-emitted; both are absorbed by
    /// the keyed-sink dedup the tier already requires.
    Checkpoint,
}

/// What an operative does about an input that arrives behind its Entity's
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

    pub fn procedure(&self, id: ProcedureId) -> Option<&ProcedureDescriptor> {
        self.procedures.get(id as usize)
    }
}
