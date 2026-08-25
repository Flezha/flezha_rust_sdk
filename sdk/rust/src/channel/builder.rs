//! Producer-first fluent graph builder (`sdk-graph-authoring`).
//!
//! `c.input::<T>("name").into::<A>().into::<B>().sink("plugin")` — `.into`
//! wires a consumer and returns that procedure's **output stream handle**
//! ([`Stream`]), so a linear pipeline is one chained expression. Branching is
//! handle reuse (hold the handle in a variable and wire it twice) or the
//! equivalent [`Stream::branch`] sugar. Fan-in is two streams naming the same
//! consumer node.
//!
//! Compile-time enforcement:
//! - `.into::<S>()` requires `S: Procedure<Input = T>` — a type-mismatched
//!   consumer fires at the call site.
//! - Channel payload types come from the producer's declared `Output`; there
//!   is no user-supplied type token for inter-procedure channels.
//! - `.sealed()` returns a finalized [`Channel`] with no further builder
//!   methods, for the author who wants "nothing follows this" in the type.
//! - Handles borrow the builder (`Stream<'b, T>` is `Copy`), so a handle
//!   cannot outlive the declaration: using one after the topology freezes at
//!   registration is a compile error, not a runtime failure.

use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;

use crate::channel::{
    Channel, ChannelId, ChannelKind, ConsumerEdge, EdgeKeyFn, EdgeTransformFn, EdgeYield,
    OverflowPolicy, ProcedureId,
};
use crate::procedure::Procedure;

/// Type-erased dispatcher factory for one Procedure. Stored on
/// `BuilderState::procedures` and consumed by `build_topology` to produce
/// per-entity slots in each `StreamInstance`.
pub(crate) struct ProcedureEntry {
    /// Node identity: the procedure's `name()`, or the explicit node name
    /// given at wiring time (`into_named`). Feeds everything `name()` feeds —
    /// state directories, journal entries, the derived channel name,
    /// diagnostics.
    pub name: &'static str,
    /// The Rust type behind this node, for the unification rule: two unnamed
    /// mentions of one class are the same node; a named mention is its own.
    pub class_id: TypeId,
    /// Whether this node was created with an explicit node name.
    pub named: bool,
    pub entity_keys: Vec<&'static str>,
    pub entities_for_overridden: bool,
    /// `Procedure::COMPENSATE_OVERRIDDEN` captured at registration time. The
    /// workflow driver consults the resolved descriptor flag to fire saga
    /// compensation only for procedures that declared a `compensate` hook.
    pub compensate_overridden: bool,
    /// Channels this node consumes. Several = a fan-in, or a merge whose
    /// edges each map into this node's `Input`: either way the node receives
    /// the accumulated contributions of all its producers.
    pub input_channel_ids: Vec<u32>,
    pub output_channel_id: Option<u32>,
    /// Erased input-type marker: matches the consumer-edge's
    /// `consumer_input_type_id` for compile-time-erased correctness checks.
    pub input_type_id: std::any::TypeId,
    /// Erased output-type marker: matches the producer-edge's payload type.
    pub output_type_id: std::any::TypeId,

    /// `Procedure::Clock::type_name()` captured at registration time, plus
    /// whether the Procedure's `Change` is `NoChange`. `build_topology`
    /// rejects clock configurations the runtime cannot serve (a
    /// `LogicalClock` without persistent changes never advances).
    pub clock_type_name: &'static str,
    /// `Clock::IS_EVENT_TIME` — whether this procedure's clock yields a time
    /// carried by the input rather than one read off the wall or the WAL.
    /// Late-input correction is only definable against such a clock.
    pub clock_is_event_time: bool,
    pub change_is_nochange: bool,

    /// `Procedure::mailbox_capacity()` captured at registration time. The
    /// resolved value (after applying env-var precedence in `build_topology`)
    /// is the size of every Entity mailbox built for this Procedure.
    pub mailbox_capacity: usize,

    /// `Procedure::MAILBOX_CAPACITY_OVERRIDDEN` captured at registration time.
    /// Drives the precedence rule in `resolve_mailbox_capacity`.
    pub mailbox_capacity_overridden: bool,

    /// `Procedure::compact_after_bytes()` captured at registration time. The
    /// controller resolves the per-Entity WAL size compaction trigger from it,
    /// shipped in the `AddOperative` response metadata.
    pub compact_after_bytes: usize,

    /// Build a per-entity slot. The closure captures the Procedure type
    /// statically so the entity mailbox carries `Arc<S::Input>` items
    /// and `Procedure::run` is called with the typed signature.
    pub build_entity: EntityBuilder,

    /// Entity routing for inputs targeting this Procedure. Returns the
    /// entity keys. Single-mode procedures return an empty vec; the runtime
    /// then routes everything to entity index 0.
    pub entities_for_erased: EntitiesForFn,

    /// Dynamic-mode knobs captured at registration (`Procedure::entity_idle_ttl`,
    /// `max_entities`). Carried through to the `ProcedureDescriptor` for the
    /// reaper / registry; inert for non-dynamic Procedures. `idle_ttl == None`
    /// means no eviction.
    pub idle_ttl: Option<std::time::Duration>,
    pub max_entities: usize,

    /// Serialize an Entity's typed state to JSON. Captures S statically
    /// so the closure downcasts the type-erased `Box<dyn Any + Send>`
    /// stored in `EntitySlot::state` and produces a `serde_json::Value`
    /// via the `S::State: Serialize` bound on the trait.
    pub serialize_state: SerializeStateFn,

    /// Deserialize a per-Procedure JSON value into a fresh State and wrap
    /// it as the typed-erased payload `EntitySlot::state` expects
    /// (a `Box<EntityState<S>>`). Captures S statically; constructs
    /// a fresh `S` via `Default` since the user's `Procedure` is stateless.
    pub deserialize_state: DeserializeStateFn,
}

/// One author callback erased behind `&dyn Any` — how every typed declaration
/// is stored, whatever it returns. Built by [`erase`]; the public
/// `EdgeTransformFn`, `EdgeKeyFn` and `PluginSerializeFn` are this same shape
/// spelled out.
pub use flz_core::channel::{
    BuildEntityArgs, DeserializeStateFn, EntitiesForFn, EntityBuilder, ErasedFn, SerializeStateFn,
};

/// Plugin sink attached to a channel via `sink(...)` / `sink_with(...)` — the
/// one representation of plugin egress. A channel that also has consumers is
/// hybrid; one that has none is the plugin-bound terminal, and
/// `build_topology` derives that `ChannelKind` from the wiring rather than
/// from which method was called.
pub(crate) struct PluginSink {
    pub name: &'static str,
    pub serializer: crate::channel::PluginSerializeFn,
}

/// Internal channel record used during topology construction.
pub(crate) struct ChannelEntry {
    pub name: &'static str,
    pub kind: ChannelKind,
    pub producer: Option<ProcedureId>,
    pub consumers: Vec<ConsumerEdge>,
    pub wrap_input: Option<crate::channel::EntryWrapFn>,
    /// Entry channels whose `wrap_input` is a real parser (typed JSON or a
    /// custom `input_with` parser). A failing parse rides the channel's
    /// retry→DLQ ladder; `Vec<u8>`/`String` passthrough entries keep the
    /// pre-change drop-and-log behaviour on invalid input.
    pub entry_parsed: bool,
    /// The channel's plugin egress, populated by `sink(...)` / `sink_with(...)`.
    pub plugin_sink: Option<PluginSink>,
    /// Output-identity opt-in set by `.keyed()`. Valid only on channels
    /// with a plugin wire; `build_topology` rejects a keyed channel that
    /// has no `sink`.
    pub keyed: bool,
    /// What this channel discards when the consumer mailbox is full,
    /// applied at the push site. Initialized at channel-creation time to
    /// `OverflowPolicy::default()` (`DropNothing`, every kind); a channel
    /// with no consumers carries a placeholder that is never consulted,
    /// because a plugin emit writes straight to the wire. `.on_full(...)`
    /// overwrites this value.
    pub overflow_policy: OverflowPolicy,
    /// Retry-then-park policy for this channel's consumer dispatchers.
    /// `.retry_max(...)` / `.retry_backoff_ms(...)` overwrite the defaults.
    pub retry: crate::channel::RetryPolicy,
    /// Inter-proc byte codec declared via `.codec()` / `.codec_with(...)`.
    /// Required for durable structured workflow classes (`workflow-proc-journal`).
    pub codec: Option<crate::channel::ChannelCodec>,
}

/// Reasons `StreamOperative::channels(c)` declarations fail at registration time.
#[derive(Debug, thiserror::Error)]
pub enum TopologyIssue {
    #[error("channel `{0}` declared more than once on this operative")]
    DuplicateChannel(&'static str),

    #[error(
        "node identity `{name}` is declared by two distinct nodes; a node name must be unique \
         per operative and distinct from every procedure name()"
    )]
    NodeIdentityCollision { name: &'static str },

    #[error("branch(...) requires at least two arms; {arms} given")]
    BranchTooFewArms { arms: usize },

    #[error(
        "procedure `{procedure}` is on a cycle; an operative topology is a DAG — a node's \
         output may never reach its own input"
    )]
    Cycle { procedure: &'static str },

    #[error("procedure `{procedure}` is not reachable from any entry channel")]
    UnreachableProcedure { procedure: &'static str },

    #[error(
        "procedure `{name}` declares non-empty entity_keys() but does not override entities_for \
         (set ENTITIES_FOR_OVERRIDDEN = true)"
    )]
    EntitiesForMissing { name: &'static str },

    #[error(
        "channel `{channel}` declares .key_by(...) into procedure `{procedure}`, which declares a \
         non-empty entity_keys() (static mode); static keying routes by entities_for and may name \
         several Entities, edge keying names exactly one — declare one OR the other, not both"
    )]
    KeyByOnUnkeyableTarget {
        channel: &'static str,
        procedure: &'static str,
    },

    #[error("channel `{channel}` declares .key_by(...) twice before one `.into(...)`")]
    DuplicateKeyBy { channel: &'static str },

    #[error(
        "channel `{channel}` declares a transform or .key_by(...) after a plugin sink; the sink \
         serializes the channel's own value, so nothing downstream of it can be reshaped"
    )]
    TransformAfterTerminal { channel: &'static str },

    #[error(
        "channel `{channel}` declares a transform after .key_by(...); the key is evaluated on the \
         final post-transform value, so it must be the last link before `.into(...)`"
    )]
    TransformAfterKeyBy { channel: &'static str },

    #[error(
        "channel `{channel}` declares a transform or .key_by(...) that is never closed by \
         `.into(...)`; a transform chain is stored on the consumer edge it terminates in, so a \
         dangling one would silently do nothing"
    )]
    DanglingTransformChain { channel: &'static str },

    #[error(
        "channel `{channel}` feeds procedure `{procedure}` without a .key_by(...), but another \
         edge keys it into dynamic mode; every edge into a dynamic-mode procedure must name the \
         Entity its inputs reach"
    )]
    UnkeyedEdgeIntoDynamicProcedure {
        channel: &'static str,
        procedure: &'static str,
    },

    #[error("procedure `{name}` registered with conflicting input/output types")]
    TypeMismatch { name: &'static str },

    #[error(
        "procedure `{procedure}` declared mailbox_capacity = 0; capacity must be in 1..=1_048_576"
    )]
    MailboxCapacityZero { procedure: &'static str },

    #[error(
        "procedure `{procedure}` declared mailbox_capacity = {value}; capacity must be in 1..=1_048_576"
    )]
    MailboxCapacityTooLarge {
        procedure: &'static str,
        value: usize,
    },

    #[error(
        "channel `{channel}` was declared with `sink(...)` more than once; \
         only one plugin sink per channel is allowed"
    )]
    DuplicatePluginSink { channel: &'static str },

    #[error(
        "channel `{channel}` is declared .keyed() but has no plugin wire; output identity \
         stamps only exist on plugin-bound frames (add .sink(...))"
    )]
    KeyedChannelWithoutPluginWire { channel: &'static str },

    #[error(
        "procedure `{procedure}` declares clock `{clock_type}` with `type Change = NoChange`; \
         the runtime cannot serve it (logical time is the durable change seq, which never \
         advances without persistent changes)"
    )]
    UnservableClock {
        procedure: &'static str,
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
         (use ProcedureError::non_retryable to park without retries)"
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
        "operative declares correction_window() but procedure `{procedure}` declares clock \
         `{clock_type}`; correction orders a replayed slice by event time, which only \
         `EventTimeFrom` yields"
    )]
    CorrectionWithoutEventTime {
        procedure: &'static str,
        clock_type: &'static str,
    },

    #[error("operative declares correction_window(0); a zero window can correct nothing")]
    CorrectionWindowZero,

    #[error(
        "operative declares checkpoint_every(0); an interval that closes every zero inputs is \
         not an interval — omit it to close only on an explicit ctx.checkpoint()"
    )]
    CheckpointEveryZero,

    #[error(
        "operative declares checkpoint_ack() without effectively_once(); the checkpoint is \
         where the tier's consumed-offset advances, and an at-least-once operative has no \
         durable offset to hold back (nor a durable input log to replay the interval from)"
    )]
    CheckpointAckWithoutTier,

    #[error(
        "merge into procedure `{procedure}` declares {sources} source channel(s); a merge \
         consumes two or more — wire a single source with `.into::<Proc>()` instead"
    )]
    MergeTooFewSources {
        procedure: &'static str,
        sources: usize,
    },

    #[error(
        "merge subscribes channel `{channel}` twice; each merge source must be a distinct \
         channel (one mapping per channel into the merged input)"
    )]
    DuplicateMergeSource { channel: &'static str },

    #[error(
        "channel `{channel}` already feeds procedure `{procedure}`; an edge carries exactly one \
         transform and one key or neither, so a re-declaration where either side carries one is \
         refused rather than silently keeping the first"
    )]
    EdgeAlreadyWired {
        channel: &'static str,
        procedure: &'static str,
    },
}

/// A transform/key chain declared on a stream handle but not yet closed by
/// `.into(...)`. Every `.map` / `.filter` / `.flat_map` / `.key_by` pushes a
/// NEW entry composing the previous one, which is what keeps the handles
/// `Copy` (they carry an index, not an `Arc`) and what makes reusing a handle
/// mid-chain branch cleanly instead of aliasing.
pub(crate) struct PendingEdge {
    /// The channel the chain is declared on — the diagnostic's subject.
    pub channel: ChannelId,
    /// Every transform declared so far, composed into ONE closure. Composition
    /// is the whole of "fusion": the runtime applies one closure per edge and
    /// never learns that a chain existed.
    pub transform: Option<EdgeTransformFn>,
    pub key: Option<EdgeKeyFn>,
    /// Set when a later link built on this one, so only a chain's tip can be
    /// reported dangling.
    pub superseded: bool,
    /// Set when `.into(...)` (or a merge) closed the chain onto an edge.
    pub consumed: bool,
}

/// Mutable topology under construction, behind the builder's `RefCell` so the
/// `Copy` stream handles can share it.
pub(crate) struct BuilderState {
    pub channels: Vec<ChannelEntry>,
    pub procedures: Vec<ProcedureEntry>,
    pub procedure_name_to_id: HashMap<&'static str, u32>,
    /// Transform/key chains under construction, indexed by the id the stream
    /// handles carry. A chain neither superseded nor consumed is dangling.
    pub pending: Vec<PendingEdge>,
    pub errors: Vec<TopologyIssue>,
    pub effectively_once: bool,
    pub correction: Option<crate::operative::topology::CorrectionPolicy>,
    pub ack_granularity: crate::operative::topology::AckGranularity,
}

/// Builder collected by `StreamOperative::channels`. Internal state turns into a
/// frozen `OperativeTopology` at `build_topology` time. Stream handles hold a
/// shared reference to the builder, so its interior is a `RefCell`.
pub struct ChannelBuilder {
    pub(crate) state: RefCell<BuilderState>,
}

impl ChannelBuilder {
    pub(crate) fn new() -> Self {
        Self {
            state: RefCell::new(BuilderState {
                channels: Vec::new(),
                procedures: Vec::new(),
                procedure_name_to_id: HashMap::new(),
                pending: Vec::new(),
                errors: Vec::new(),
                effectively_once: false,
                correction: None,
                ack_granularity: crate::operative::topology::AckGranularity::PerInput,
            }),
        }
    }

    /// Freeze: hand the collected state to `build_topology`. Consuming the
    /// builder here is what invalidates every outstanding stream handle — they
    /// borrow it, so a handle used after the freeze is a compile error.
    pub(crate) fn into_state(self) -> BuilderState {
        self.state.into_inner()
    }

    /// Whether any procedure was declared (`false` selects the workflow
    /// run-sugar path).
    pub(crate) fn has_procedures(&self) -> bool {
        !self.state.borrow().procedures.is_empty()
    }

    /// Opt this operative into late-input correction over the given window.
    ///
    /// An input whose extracted event time is behind its Entity's high-water
    /// by no more than `window_ms` is not processed as an arrival. The engine
    /// rewinds the Entity to a baseline preceding that event time, replays
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
    pub fn correction_window(&self, window_ms: u64) {
        let mut st = self.state.borrow_mut();
        let beyond_window = st.correction.map(|c| c.beyond_window).unwrap_or_default();
        st.correction = Some(crate::operative::topology::CorrectionPolicy {
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
    pub fn beyond_window_policy(&self, policy: crate::operative::topology::BeyondWindowPolicy) {
        let mut st = self.state.borrow_mut();
        let window_ms = st.correction.map(|c| c.window_ms).unwrap_or(0);
        st.correction = Some(crate::operative::topology::CorrectionPolicy {
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
    pub fn effectively_once(&self) {
        self.state.borrow_mut().effectively_once = true;
    }

    /// Advance the durable consumed-offset only at an explicit
    /// [`ProcedureCtx::checkpoint`], instead of once per input.
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
    /// checkpointing moves the *acknowledgement*, not the output. To hold them
    /// for the interval too, see [`outputs_at_checkpoint`].
    ///
    /// [`ProcedureCtx::checkpoint`]: crate::procedure::ProcedureCtx::checkpoint
    /// [`outputs_at_checkpoint`]: ChannelBuilder::outputs_at_checkpoint
    pub fn checkpoint_ack(&self) {
        self.checkpoint_policy(|_policy| {});
    }

    /// Close the checkpoint interval once `inputs` inputs have folded into it,
    /// instead of waiting for an explicit [`ProcedureCtx::checkpoint`].
    ///
    /// An explicit checkpoint still closes the interval early; this is the
    /// ceiling, not the only closer. Implies `checkpoint_ack()`.
    ///
    /// [`ProcedureCtx::checkpoint`]: crate::procedure::ProcedureCtx::checkpoint
    pub fn checkpoint_every(&self, inputs: u32) {
        self.checkpoint_policy(|policy| policy.every = Some(inputs));
    }

    /// Hold the interval's outputs with its changes and ship them inside its
    /// checkpoint record, which the controller flushes as one batch after that
    /// record is fsynced — one WAL append per interval instead of one per
    /// invocation.
    ///
    /// The trade is output latency: an emit is not visible downstream until
    /// its interval closes. `max_delay_ms` bounds that — the interval closes
    /// that long after the input that opened it even if nothing else arrives,
    /// so a quiet Entity never holds an output indefinitely. It is a parameter
    /// rather than an option because an unbounded hold is not a posture anyone
    /// should be able to declare by omission.
    ///
    /// Replay is exactly the mode's existing contract: an interval that did not
    /// checkpoint re-produces its outputs when it replays, a checkpointed
    /// record re-drives with identical identity, and the keyed-sink dedup the
    /// tier already requires absorbs both. Implies `checkpoint_ack()`.
    pub fn outputs_at_checkpoint(&self, max_delay_ms: u64) {
        self.checkpoint_policy(|policy| {
            policy.outputs = crate::operative::topology::CheckpointOutputs::AtCheckpoint;
            policy.max_delay_ms = Some(max_delay_ms);
        });
    }

    /// Turn the checkpoint granularity on (idempotent) and amend its policy,
    /// so each option is one call that preserves the others.
    fn checkpoint_policy(
        &self,
        amend: impl FnOnce(&mut crate::operative::topology::CheckpointPolicy),
    ) {
        use crate::operative::topology::{AckGranularity, CheckpointPolicy};

        let mut st = self.state.borrow_mut();
        let mut policy = st
            .ack_granularity
            .checkpoint()
            .unwrap_or(CheckpointPolicy::default());
        amend(&mut policy);
        st.ack_granularity = AckGranularity::Checkpoint(policy);
    }

    /// Declare the entry channel fed by the controller — the operative's
    /// external input address, so its name is explicit and mandatory.
    ///
    /// Any payload type is accepted. `Vec<u8>` is delivered unchanged and
    /// `String` UTF-8-decoded (invalid UTF-8 is dropped and logged), exactly
    /// as before; any other `T` is deserialized at the entry edge from the
    /// wire bytes with the SDK's default JSON codec (`serde_json`). A failing
    /// parse is classified like a failing invocation and rides this channel's
    /// retry→DLQ ladder. For a non-JSON wire format, use
    /// [`input_with`](ChannelBuilder::input_with).
    pub fn input<T: DeserializeOwned + Send + Sync + 'static>(
        &self,
        name: &'static str,
    ) -> EntryStream<'_, T> {
        let (wrap, parsed) = default_entry_wrap::<T>();
        self.entry_channel(name, wrap, parsed)
    }

    /// Declare the entry channel with a custom `bytes → T` parser, replacing
    /// the default JSON codec of [`input`](ChannelBuilder::input).
    ///
    /// The parser MUST be deterministic — the same bytes always produce the
    /// same value, with no ambient reads (time, randomness, I/O) — the same
    /// contract output serializers carry. A returned `Err` is classified like
    /// a failing invocation and rides this channel's retry→DLQ ladder.
    pub fn input_with<T, P>(&self, name: &'static str, parser: P) -> EntryStream<'_, T>
    where
        T: Send + Sync + 'static,
        P: Fn(Vec<u8>) -> Result<T, String> + Send + Sync + 'static,
    {
        let wrap: crate::channel::EntryWrapFn = Arc::new(move |bytes: Vec<u8>| {
            parser(bytes).map(|v| Arc::new(v) as Arc<dyn Any + Send + Sync>)
        });
        self.entry_channel(name, wrap, true)
    }

    /// Open a **merged** subscription: one procedure fed by two or more source
    /// channels, each supplying a mapping from its payload into the
    /// procedure's `Input` sum type (`multi-input-procedures`).
    ///
    /// ```ignore
    /// c.merge::<SignalInput>()
    ///     .from(ticker, SignalInput::Ticker)
    ///     .from(config, SignalInput::Config)
    ///     .into::<DetectorProcedure>();
    /// ```
    ///
    /// Sources may be entry streams (sealed payloads) or another procedure's
    /// output stream, freely mixed — on an **stream** operative. A workflow
    /// operative is seeded from its spawn args through one entry channel and
    /// nothing on that path says which channel a payload arrived on, so a
    /// workflow merge over an entry channel is refused at registration
    /// (`BuildWorkflowError::MergeFromEntryChannel`); its sources are the
    /// output streams of two or more of its procedures.
    ///
    /// `entities_for` and the merge's own `key_by` are evaluated on the
    /// **mapped** value, so per-source entity mode is encoded in the sum type.
    /// A source handle may carry its own transforms, which run before that
    /// source's mapping; keying is declared once, on the merge. Per-source FIFO
    /// order is preserved; nothing is guaranteed across sources — correlate in
    /// the procedure's own state.
    ///
    /// The mappings MUST be pure: they run on live delivery and again on WAL
    /// replay, and a replay that disagreed with the live run would restore a
    /// state the live run never had. They must also be total — a mapping
    /// returns `I`, not `Result<I, _>`, so unlike an entry parser it has no
    /// error to hand the channel's retry→DLQ ladder. Do the fallible work in
    /// the procedure, on the sum type.
    ///
    /// A panicking mapping is contained everywhere it can be: on an
    /// inter-procedure source it is caught as the producing procedure's panic
    /// (stream) or turned into a terminal chain error the saga compensates
    /// (workflow). The one unguarded seam is a stream entry-channel source,
    /// where the mapping runs on the wire-reader path — the same exposure the
    /// entry parser there already carries. `docs/sdk/multi_input_procedures.md`
    /// tabulates it.
    ///
    /// A source's payload must be [`Clone`]: the mapping is handed the
    /// channel's value by reference and clones it, where a plain `.into`
    /// forwards the existing `Arc`.
    ///
    /// `Procedure::Input == I` is enforced at the `.into::<Proc>()` call site by
    /// the compiler. Fewer than two sources, the same channel twice, or a
    /// source whose edge into this procedure is already wired all fail at
    /// registration ([`TopologyIssue::MergeTooFewSources`] /
    /// [`TopologyIssue::DuplicateMergeSource`] /
    /// [`TopologyIssue::EdgeAlreadyWired`]).
    pub fn merge<I: Send + Sync + 'static>(&self) -> Merge<'_, I> {
        Merge {
            builder: self,
            sources: Vec::new(),
            declared: 0,
            key: None,
            duplicate_key: false,
            _phantom: PhantomData,
        }
    }

    fn entry_channel<T: Send + Sync + 'static>(
        &self,
        name: &'static str,
        wrap: crate::channel::EntryWrapFn,
        entry_parsed: bool,
    ) -> EntryStream<'_, T> {
        let mut st = self.state.borrow_mut();
        let id = st.push_channel(name, ChannelKind::Entry);
        let entry = &mut st.channels[id as usize];
        entry.wrap_input = Some(wrap);
        entry.entry_parsed = entry_parsed;
        EntryStream {
            builder: self,
            target: id,
            pending: None,
            _phantom: PhantomData,
        }
    }
}

impl BuilderState {
    fn push_channel(&mut self, name: &'static str, kind: ChannelKind) -> u32 {
        let id = self.channels.len() as u32;
        self.channels.push(ChannelEntry {
            name,
            kind,
            producer: None,
            consumers: Vec::new(),
            wrap_input: None,
            entry_parsed: false,
            plugin_sink: None,
            keyed: false,
            overflow_policy: OverflowPolicy::default(),
            retry: crate::channel::RetryPolicy::default(),
            codec: None,
        });
        id
    }

    /// Resolve or create the node for `S` under the node-identity rule: two
    /// unnamed mentions of one class are the same node; an explicit node name
    /// creates/refers to a distinct node whose identity string replaces
    /// `S::name()`. An identity claimed by a *different* declaration is a
    /// collision.
    fn register_node<S: Procedure>(&mut self, node_name: Option<&'static str>) -> u32 {
        let identity = node_name.unwrap_or_else(S::name);
        let named = node_name.is_some();
        if let Some(&id) = self.procedure_name_to_id.get(identity) {
            let existing = &self.procedures[id as usize];
            if existing.class_id != TypeId::of::<S>() || existing.named != named {
                self.errors
                    .push(TopologyIssue::NodeIdentityCollision { name: identity });
            }
            return id;
        }
        let id = self.procedures.len() as u32;

        let entities_for_erased: EntitiesForFn =
            erase::<S::Input, _, _>("entities_for", S::entities_for);

        let build_entity: EntityBuilder = Box::new(
            |args: BuildEntityArgs<'_>| -> Arc<crate::entity::EntitySlot> {
                crate::operative::dispatcher::build_entity_slot::<S>(args)
            },
        );

        let (serialize_state, deserialize_state) =
            crate::operative::dispatcher::build_state_fns::<S>();

        self.procedures.push(ProcedureEntry {
            name: identity,
            class_id: TypeId::of::<S>(),
            named,
            entity_keys: S::entity_keys(),
            entities_for_overridden: S::ENTITIES_FOR_OVERRIDDEN,
            compensate_overridden: S::COMPENSATE_OVERRIDDEN,
            input_channel_ids: Vec::new(),
            output_channel_id: None,
            input_type_id: std::any::TypeId::of::<S::Input>(),
            output_type_id: std::any::TypeId::of::<S::Output>(),
            clock_type_name: <S::Clock as crate::procedure::Clock>::type_name(),
            clock_is_event_time: <S::Clock as crate::procedure::Clock>::IS_EVENT_TIME,
            change_is_nochange: std::any::TypeId::of::<S::Change>()
                == std::any::TypeId::of::<crate::procedure::NoChange>(),
            mailbox_capacity: S::mailbox_capacity(),
            mailbox_capacity_overridden: S::MAILBOX_CAPACITY_OVERRIDDEN,
            compact_after_bytes: S::compact_after_bytes(),
            build_entity,
            entities_for_erased,
            idle_ttl: S::entity_idle_ttl(),
            max_entities: S::max_entities(),
            serialize_state,
            deserialize_state,
        });
        self.procedure_name_to_id.insert(identity, id);
        id
    }

    /// The node's output channel, created lazily on first use so a leaf node
    /// never carries a phantom channel. Named by the node's identity until a
    /// `.named(...)` override.
    fn ensure_output_channel(&mut self, node: u32) -> u32 {
        if let Some(id) = self.procedures[node as usize].output_channel_id {
            return id;
        }
        let name = self.procedures[node as usize].name;
        let id = self.push_channel(name, ChannelKind::InterProcedure);
        self.channels[id as usize].producer = Some(node);
        self.procedures[node as usize].output_channel_id = Some(id);
        id
    }

    /// Wire `channel_id` into a consumer node (created/resolved by identity),
    /// closing `pending`'s transform chain onto the resulting edge.
    fn wire_consumer<S: Procedure>(
        &mut self,
        channel_id: u32,
        pending: Option<u32>,
        node_name: Option<&'static str>,
    ) -> u32 {
        let consumer = self.register_node::<S>(node_name);
        self.attach_consumer(channel_id, consumer, pending);
        consumer
    }

    /// Record one consumer subscription on a channel, closing `pending`'s
    /// transform/key chain onto it — a `merge::<I>().from(..)` mapping is one
    /// link of that chain. `None` is the bare `.into`, where the channel's
    /// payload already IS the consumer's `Input` and the consumer's Entity
    /// mode is its own. Closing the chain here is what makes it impossible to
    /// wire an edge and forget its closures.
    ///
    /// Re-declaring an existing bare edge is the fan-in / repeated `.into`
    /// case and stays a no-op. Re-declaring one where either side carries a
    /// transform or a key is refused rather than silently dropped — the edge
    /// would otherwise keep whichever closure was declared first and deliver
    /// the wrong value, to the wrong Entity, forever.
    fn attach_consumer(&mut self, channel_id: u32, consumer: u32, pending: Option<u32>) {
        let (transform, key) = self.close_pending(pending);
        let entry = &mut self.channels[channel_id as usize];
        let declares = transform.is_some() || key.is_some();
        match entry.consumers.iter().find(|e| e.procedure == consumer) {
            Some(existing)
                if existing.transform.is_some() || existing.key.is_some() || declares =>
            {
                let channel = entry.name;
                let procedure = self.procedures[consumer as usize].name;
                self.errors
                    .push(TopologyIssue::EdgeAlreadyWired { channel, procedure });
                return;
            }
            Some(_) => {}
            None => entry.consumers.push(ConsumerEdge {
                procedure: consumer,
                transform,
                key,
            }),
        }
        let proc = &mut self.procedures[consumer as usize];
        if !proc.input_channel_ids.contains(&channel_id) {
            proc.input_channel_ids.push(channel_id);
        }
    }

    /// Extend the chain at `base` (or open one on `channel`) with one more
    /// link, returning the new chain's id. Composing here — rather than storing
    /// a list the runtime walks — is what makes fusion structural: whatever the
    /// author chained, the edge ends up with exactly one closure.
    fn extend_pending(
        &mut self,
        channel: ChannelId,
        base: Option<u32>,
        link: Option<EdgeTransformFn>,
        key: Option<EdgeKeyFn>,
    ) -> u32 {
        if self.channels[channel as usize].plugin_sink.is_some() {
            let channel_name = self.channels[channel as usize].name;
            self.errors.push(TopologyIssue::TransformAfterTerminal {
                channel: channel_name,
            });
        }
        let (prior_transform, prior_key) = match base {
            Some(id) => {
                self.pending[id as usize].superseded = true;
                let prior = &self.pending[id as usize];
                (prior.transform.clone(), prior.key.clone())
            }
            None => (None, None),
        };
        // The key is evaluated on the final post-transform value, so a
        // transform declared after it would hand it a value of a type it was
        // never written against.
        if prior_key.is_some() && link.is_some() {
            let channel_name = self.channels[channel as usize].name;
            self.errors.push(TopologyIssue::TransformAfterKeyBy {
                channel: channel_name,
            });
        }
        if prior_key.is_some() && key.is_some() {
            let channel_name = self.channels[channel as usize].name;
            self.errors.push(TopologyIssue::DuplicateKeyBy {
                channel: channel_name,
            });
        }
        let id = self.pending.len() as u32;
        self.pending.push(PendingEdge {
            channel,
            transform: compose(prior_transform, link),
            key: key.or(prior_key),
            superseded: false,
            consumed: false,
        });
        id
    }

    /// Take a chain's closures and mark it closed. `None` (a bare handle) is
    /// the transform-free case every pre-existing topology is.
    fn close_pending(
        &mut self,
        pending: Option<u32>,
    ) -> (Option<EdgeTransformFn>, Option<EdgeKeyFn>) {
        let Some(id) = pending else {
            return (None, None);
        };
        let chain = &mut self.pending[id as usize];
        chain.consumed = true;
        (chain.transform.clone(), chain.key.clone())
    }
}

/// Compose two edge transforms into one. `Drop` short-circuits, a fan-out
/// feeds each of its values through the next link and concatenates — so a
/// `flat_map` followed by a `filter` yields exactly the surviving values.
fn compose(
    first: Option<EdgeTransformFn>,
    second: Option<EdgeTransformFn>,
) -> Option<EdgeTransformFn> {
    match (first, second) {
        (None, next) => next,
        (prev, None) => prev,
        (Some(prev), Some(next)) => {
            Some(Arc::new(
                move |value: &Arc<dyn Any + Send + Sync>| match prev(value) {
                    EdgeYield::Drop => EdgeYield::Drop,
                    EdgeYield::One(v) => next(&v),
                    EdgeYield::Many(vs) => {
                        let mut out = Vec::with_capacity(vs.len());
                        for v in vs {
                            match next(&v) {
                                EdgeYield::Drop => {}
                                EdgeYield::One(y) => out.push(y),
                                EdgeYield::Many(ys) => out.extend(ys),
                            }
                        }
                        EdgeYield::Many(out)
                    }
                },
            ))
        }
    }
}

/// Apply a `.retry_max(...)` declaration, rejecting a zero budget at
/// registration time (surfaced by `build_topology` like every other
/// declaration error).
fn set_retry_max(st: &mut BuilderState, channel_id: u32, max_attempts: u32) {
    let entry = &mut st.channels[channel_id as usize];
    if max_attempts == 0 {
        let channel = entry.name;
        st.errors.push(TopologyIssue::RetryMaxZero { channel });
        return;
    }
    entry.retry.max_attempts = max_attempts;
}

/// Install the standard inter-proc byte codec for `T` — postcard over the
/// SDK's standard serde derives, the same wire format the effect records
/// use. Shared by the `.codec()` declarations on both builder stages.
fn set_codec<T>(st: &mut BuilderState, channel_id: u32)
where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    set_codec_with::<T, _, _>(
        st,
        channel_id,
        |typed: &T| {
            postcard::to_allocvec(typed).map_err(|e| format!("inter-proc codec encode failed: {e}"))
        },
        |bytes: Vec<u8>| {
            postcard::from_bytes::<T>(&bytes)
                .map_err(|e| format!("inter-proc codec decode failed: {e}"))
        },
    );
}

/// Install a custom typed→bytes / bytes→typed codec pair, type-erasing both
/// halves onto the channel entry.
fn set_codec_with<T, E, D>(st: &mut BuilderState, channel_id: u32, encode: E, decode: D)
where
    T: Send + Sync + 'static,
    E: Fn(&T) -> Result<Vec<u8>, String> + Send + Sync + 'static,
    D: Fn(Vec<u8>) -> Result<T, String> + Send + Sync + 'static,
{
    let encode_erased: crate::channel::CodecEncodeFn = Arc::new(move |val: &dyn Any| {
        let typed = val
            .downcast_ref::<T>()
            .ok_or_else(|| "inter-proc codec: type mismatch (build-time invariant)".to_string())?;
        encode(typed)
    });
    let decode_erased: crate::channel::EntryWrapFn = Arc::new(move |bytes: Vec<u8>| {
        decode(bytes).map(|v| Arc::new(v) as Arc<dyn Any + Send + Sync>)
    });
    st.channels[channel_id as usize].codec = Some(crate::channel::ChannelCodec {
        encode: encode_erased,
        decode: decode_erased,
    });
}

/// The default entry decode for `T`: passthrough for `Vec<u8>`, UTF-8 for
/// `String` (both byte-identical to the pre-typed-entry contract), the SDK's
/// default JSON codec otherwise. The bool is the "real parser" flag driving
/// the retry→DLQ classification of a failing decode.
fn default_entry_wrap<T: DeserializeOwned + Send + Sync + 'static>(
) -> (crate::channel::EntryWrapFn, bool) {
    if TypeId::of::<T>() == TypeId::of::<Vec<u8>>() {
        (
            Arc::new(|bytes: Vec<u8>| Ok(Arc::new(bytes) as Arc<dyn Any + Send + Sync>)),
            false,
        )
    } else if TypeId::of::<T>() == TypeId::of::<String>() {
        (
            Arc::new(|bytes: Vec<u8>| match String::from_utf8(bytes) {
                Ok(s) => Ok(Arc::new(s) as Arc<dyn Any + Send + Sync>),
                Err(e) => Err(format!("entry channel: invalid UTF-8: {}", e)),
            }),
            false,
        )
    } else {
        (
            Arc::new(|bytes: Vec<u8>| {
                serde_json::from_slice::<T>(&bytes)
                    .map(|v| Arc::new(v) as Arc<dyn Any + Send + Sync>)
                    .map_err(|e| format!("entry channel: JSON deserialization failed: {e}"))
            }),
            true,
        )
    }
}

/// The default plugin serialization for `T`: passthrough for `Vec<u8>`, UTF-8
/// bytes for `String`, the SDK's default JSON codec otherwise — what
/// `sink(...)` applies when the author supplies no serializer.
fn default_plugin_serializer<T: serde::Serialize + Send + Sync + 'static>(
) -> crate::channel::PluginSerializeFn {
    Arc::new(|val: &dyn Any| -> Vec<u8> {
        if let Some(bytes) = val.downcast_ref::<Vec<u8>>() {
            return bytes.clone();
        }
        if let Some(s) = val.downcast_ref::<String>() {
            return s.as_bytes().to_vec();
        }
        let typed = val
            .downcast_ref::<T>()
            .expect("plugin serializer: type mismatch (build-time invariant)");
        serde_json::to_vec(typed).unwrap_or_else(|e| {
            eprintln!("[flezha] default plugin serialization failed: {e}");
            Vec::new()
        })
    })
}

/// A channel handle usable as a [`merge`](ChannelBuilder::merge) source:
/// either an [`EntryStream`] (sealed payload off the wire) or a [`Stream`]
/// (another procedure's output). One impl covers both, so the two are freely
/// mixed in one merge.
pub trait MergeSource<'b> {
    /// The payload the source channel carries — the mapping's argument type,
    /// after any transform declared on the source handle.
    type Payload: Clone + Send + Sync + 'static;

    /// The channel this source subscribes on and the transform chain declared
    /// on the handle, materializing a producer's output channel if it did not
    /// exist yet. The mapping composes *after* that chain, which is why a
    /// transform on a source sees the source's own payload type.
    fn merge_site(self) -> (ChannelId, Option<u32>);
}

impl<'b, T: Clone + Send + Sync + 'static, K: StreamTarget> MergeSource<'b> for Stream<'b, T, K> {
    type Payload = T;

    fn merge_site(self) -> (ChannelId, Option<u32>) {
        (self.with_channel(|_, cid| cid), self.pending)
    }
}

/// A merged subscription under construction: the sources declared so far for
/// one consumer whose `Input` is the sum type `I`. Built by
/// [`ChannelBuilder::merge`] and closed by [`into`](Merge::into).
///
/// Nothing is recorded on the topology until `into`, so a merge left unclosed
/// declares no edges at all — silently. `must_use` is what makes that a
/// compiler warning rather than a graph that is quietly missing a consumer.
#[must_use = "a merge declares nothing until it is closed with `.into::<Proc>()`"]
pub struct Merge<'b, I> {
    builder: &'b ChannelBuilder,
    /// Per source: its channel and the pending chain that ends in this merge's
    /// mapping.
    sources: Vec<(ChannelId, u32)>,
    /// Every `.from` the author wrote, including ones already refused. The
    /// arity check reads this so a duplicated source is reported once, as a
    /// duplicate — not also as a merge that declared too few.
    declared: usize,
    /// The one key function a merge declares, applied to the merged value on
    /// every source's edge.
    key: Option<EdgeKeyFn>,
    duplicate_key: bool,
    _phantom: PhantomData<fn() -> I>,
}

impl<'b, I: Send + Sync + 'static> Merge<'b, I> {
    /// Add one source channel with the mapping that lifts its payload into the
    /// merged input. Subscribing the same channel twice is a registration
    /// error naming that channel.
    pub fn from<S, F>(mut self, source: S, map: F) -> Self
    where
        S: MergeSource<'b>,
        F: Fn(S::Payload) -> I + Send + Sync + 'static,
    {
        let (channel_id, source_pending) = source.merge_site();
        self.declared += 1;
        let mut st = self.builder.state.borrow_mut();
        if self
            .sources
            .iter()
            .any(|(existing, _)| *existing == channel_id)
        {
            let channel = st.channels[channel_id as usize].name;
            st.errors
                .push(TopologyIssue::DuplicateMergeSource { channel });
            return self;
        }
        let link = erase_merge_map::<S::Payload, I, F>(map);
        let chain = st.extend_pending(channel_id, source_pending, Some(link), None);
        drop(st);
        self.sources.push((channel_id, chain));
        self
    }

    /// Declare the Entity key for the merged procedure — once, on the merge,
    /// because by the time keying runs every source has been mapped into the
    /// one `Input` type. Yields the key for a value of the merged sum type.
    pub fn key_by<F>(mut self, key: F) -> Self
    where
        F: Fn(&I) -> String + Send + Sync + 'static,
    {
        if self.key.is_some() {
            self.duplicate_key = true;
        }
        self.key = Some(erase::<I, _, _>("key_by", key));
        self
    }

    /// Close the merge on its consumer and return **the consumer's** output
    /// stream handle, so a merged node chains like any other. Compile-time
    /// bound: `S::Input == I`.
    pub fn into<S: Procedure<Input = I>>(self) -> Stream<'b, S::Output> {
        self.wire::<S>(None)
    }

    /// Like [`into`](Merge::into), but the consumer is a distinct named node
    /// (the `sdk-graph-authoring` node-identity rule).
    pub fn into_named<S: Procedure<Input = I>>(
        self,
        node_name: &'static str,
    ) -> Stream<'b, S::Output> {
        self.wire::<S>(Some(node_name))
    }

    /// Close the merge on `S`, named or not — the one body behind both public
    /// spellings, and the seam a packaged building block
    /// ([`ChannelBuilder::route_latest`]) closes its own merge with instead of
    /// re-deriving the named/unnamed dispatch.
    pub(crate) fn wire<S: Procedure<Input = I>>(
        self,
        node_name: Option<&'static str>,
    ) -> Stream<'b, S::Output> {
        let mut st = self.builder.state.borrow_mut();
        let node = st.register_node::<S>(node_name);
        if self.declared < 2 {
            let procedure = st.procedures[node as usize].name;
            st.errors.push(TopologyIssue::MergeTooFewSources {
                procedure,
                sources: self.declared,
            });
        }
        if self.duplicate_key {
            if let Some(&(channel_id, _)) = self.sources.first() {
                let channel = st.channels[channel_id as usize].name;
                st.errors.push(TopologyIssue::DuplicateKeyBy { channel });
            }
        }
        for (channel_id, chain) in self.sources {
            // The key rides each source's chain, so it is evaluated on the
            // mapped value — and a key declared on a *source* handle instead
            // trips the transform-after-key refusal, which is exactly the
            // "declared once, on the merge" rule.
            let chain = match self.key.clone() {
                Some(key) => st.extend_pending(channel_id, Some(chain), None, Some(key)),
                None => chain,
            };
            st.attach_consumer(channel_id, node, Some(chain));
        }
        drop(st);
        Stream {
            builder: self.builder,
            target: node,
            pending: None,
            _phantom: PhantomData,
        }
    }
}

/// Erase one author callback — `entities_for`, a `key_by`, a hybrid plugin
/// serializer. The downcast is a build-time
/// invariant (the callback is installed by the same wiring that declared the
/// payload type), so `what` names the declaration only for the impossible
/// case. An erasure whose downcast failure is a *reported* error rather than
/// an impossibility — the inter-proc codec — needs a `Result` and stays
/// hand-written.
fn erase<T, R, F>(what: &'static str, f: F) -> ErasedFn<R>
where
    T: 'static,
    F: Fn(&T) -> R + Send + Sync + 'static,
{
    Arc::new(move |value: &dyn Any| {
        f(value
            .downcast_ref::<T>()
            .unwrap_or_else(|| panic!("{what}: type mismatch (build-time invariant)")))
    })
}

/// Erase one edge transform. The closure is handed both the downcast value and
/// the `Arc` behind it, so a surviving `filter` re-yields the original pointer
/// instead of cloning the payload.
fn erase_transform<T, F>(what: &'static str, f: F) -> EdgeTransformFn
where
    T: 'static,
    F: Fn(&T, &Arc<dyn Any + Send + Sync>) -> EdgeYield + Send + Sync + 'static,
{
    Arc::new(move |value: &Arc<dyn Any + Send + Sync>| {
        let typed = value
            .downcast_ref::<T>()
            .unwrap_or_else(|| panic!("{what}: type mismatch (build-time invariant)"));
        f(typed, value)
    })
}

/// Type-erase one merge mapping, which the author writes over an owned payload
/// (`Fn(T) -> I`) while the routing seam holds only a reference — hence the
/// clone a plain `.into` never pays.
fn erase_merge_map<T, I, F>(map: F) -> EdgeTransformFn
where
    T: Clone + Send + Sync + 'static,
    I: Send + Sync + 'static,
    F: Fn(T) -> I + Send + Sync + 'static,
{
    erase_transform::<T, _>("merge mapping", move |value: &T, _| {
        EdgeYield::One(Arc::new(map(value.clone())) as Arc<dyn Any + Send + Sync>)
    })
}

/// What a stream handle points at. The two markers are what keep the
/// entry-side and output-side handles distinct types while their shared
/// surface — transforms, keying, per-channel config, `.into` — is written
/// exactly once, against `K: StreamTarget`.
pub trait StreamTarget: Copy + 'static {
    /// Whether the handle's `target` already IS a channel id, or is the
    /// producing procedure whose output channel is created on demand.
    const IS_ENTRY: bool;
}

/// [`EntryStream`]'s marker: `target` already IS the entry channel id.
#[derive(Clone, Copy)]
pub struct EntryTarget;

/// [`Stream`]'s marker: `target` is the producing procedure, whose output
/// channel is created lazily so a leaf node never carries a phantom channel.
#[derive(Clone, Copy)]
pub struct NodeTarget;

impl StreamTarget for EntryTarget {
    const IS_ENTRY: bool = true;
}

impl StreamTarget for NodeTarget {
    const IS_ENTRY: bool = false;
}

/// The entry channel's handle: wires the graph's root consumer via `.into` and
/// carries the entry-side channel config. Same surface as [`Stream`] for
/// everything that is not producer-side (no `codec`, no plugin sink, no
/// `named` — the entry channel's name is given at `input(...)`).
pub type EntryStream<'b, T> = Stream<'b, T, EntryTarget>;

/// A procedure's output stream handle, returned by `.into::<Proc>()`: a
/// lightweight `Copy` reference into the operative's topology builder.
///
/// - `stream.into::<Next>()` wires Next as a consumer and returns Next's own
///   stream, so chains read in flow order.
/// - `map` / `filter` / `flat_map` / `key_by` reshape and key the values on the
///   way to the next `.into`, adding no node to the topology.
/// - Using one handle twice — two `.into` calls on the same variable — is
///   fan-out: every consumer receives every value released on the channel.
///   [`branch`](Stream::branch) is equivalent sugar.
/// - Two different streams wiring the same (unnamed) class fan **in**: the
///   consumer node accumulates both producers' contributions.
/// - Per-channel config (`codec`, `keyed`, `on_full`, `retry_*`, `named`)
///   chains in any order relative to consumer wiring.
///
/// The handle borrows the builder, so it cannot outlive the declaration —
/// using one after the topology freezes at registration is a compile error.
pub struct Stream<'b, T, K = NodeTarget> {
    builder: &'b ChannelBuilder,
    /// The entry channel id (`EntryTarget`) or the producing procedure id
    /// (`NodeTarget`) — `K` decides which.
    target: u32,
    /// The transform/key chain declared on this handle, `None` for a bare one.
    pending: Option<u32>,
    _phantom: PhantomData<fn() -> (T, K)>,
}

impl<'b, T, K> Clone for Stream<'b, T, K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'b, T, K> Copy for Stream<'b, T, K> {}

impl<'b, T: 'static + Send + Sync, K: StreamTarget> Stream<'b, T, K> {
    fn with_channel<R>(self, f: impl FnOnce(&mut BuilderState, u32) -> R) -> R {
        let mut st = self.builder.state.borrow_mut();
        let cid = if K::IS_ENTRY {
            self.target
        } else {
            st.ensure_output_channel(self.target)
        };
        f(&mut st, cid)
    }

    /// This handle with a new payload type and a new pending chain — the one
    /// place a transform's result handle is built.
    fn relink<U>(
        self,
        transform: Option<EdgeTransformFn>,
        key: Option<EdgeKeyFn>,
    ) -> Stream<'b, U, K> {
        let pending = self.pending;
        let id = self.with_channel(|st, cid| st.extend_pending(cid, pending, transform, key));
        Stream {
            builder: self.builder,
            target: self.target,
            pending: Some(id),
            _phantom: PhantomData,
        }
    }

    /// Wire a consumer onto this stream's channel and return **the consumer's**
    /// output stream handle, so a linear pipeline is one chained expression.
    /// Compile-time bound: `S::Input == T`. Calling `.into` on the same handle
    /// repeatedly declares fan-out; wiring the same (unnamed) class from two
    /// streams declares fan-in.
    pub fn into<S: Procedure<Input = T>>(self) -> Stream<'b, S::Output> {
        self.wire::<S>(None)
    }

    /// Like [`into`](Stream::into), but the consumer is a distinct named node:
    /// its identity string is `node_name` instead of `S::name()`, letting one
    /// Procedure class appear at several graph positions with independent
    /// state (`sdk-graph-authoring` node-identity rule).
    pub fn into_named<S: Procedure<Input = T>>(
        self,
        node_name: &'static str,
    ) -> Stream<'b, S::Output> {
        self.wire::<S>(Some(node_name))
    }

    fn wire<S: Procedure<Input = T>>(
        self,
        node_name: Option<&'static str>,
    ) -> Stream<'b, S::Output> {
        let pending = self.pending;
        let node = self.with_channel(|st, cid| st.wire_consumer::<S>(cid, pending, node_name));
        Stream {
            builder: self.builder,
            target: node,
            pending: None,
            _phantom: PhantomData,
        }
    }

    /// Reshape every value on the way to the next consumer. Costs no
    /// Procedure, no Entity, no mailbox and no dispatch hop: the closure is
    /// stored on the consumer edge this chain terminates in and evaluated at
    /// the routing seam, so it runs identically on live delivery and on the
    /// controller's re-forwarded replay slice.
    ///
    /// It MUST therefore be pure and cheap. Pure because a replay that
    /// disagreed with the live run would rebuild a state the live run never
    /// had; cheap because it runs on the routing thread — real work belongs in
    /// a Procedure behind [`key_by`](Stream::key_by), which buys per-Entity
    /// parallelism.
    pub fn map<U, F>(self, f: F) -> Stream<'b, U, K>
    where
        T: Clone,
        U: Send + Sync + 'static,
        F: Fn(T) -> U + Send + Sync + 'static,
    {
        // The owned-value spelling of `map_ref`: the clone is what the author's
        // `Fn(T) -> U` asks for, and nothing else differs.
        self.map_ref("map", move |typed: &T| f(typed.clone()))
    }

    /// The one value-reshaping link: [`map`](Stream::map) is this over an owned
    /// clone, and the packaged building blocks reach it directly to derive a
    /// small value from a payload they then discard (`route_latest`'s key
    /// function) without paying that clone. `what` names the declaration in the
    /// impossible-downcast panic, like every other erasure.
    pub(crate) fn map_ref<U, F>(self, what: &'static str, f: F) -> Stream<'b, U, K>
    where
        U: Send + Sync + 'static,
        F: Fn(&T) -> U + Send + Sync + 'static,
    {
        let link = erase_transform::<T, _>(what, move |typed: &T, _| {
            EdgeYield::One(Arc::new(f(typed)) as Arc<dyn Any + Send + Sync>)
        });
        self.relink(Some(link), None)
    }

    /// Drop the values the predicate rejects. A surviving value is forwarded
    /// by pointer — a filtered stream costs one predicate call per input and
    /// nothing else. Same purity and cost contract as [`map`](Stream::map).
    pub fn filter<F>(self, predicate: F) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let link = erase_transform::<T, _>("filter", move |typed: &T, value| {
            if predicate(typed) {
                EdgeYield::One(Arc::clone(value))
            } else {
                EdgeYield::Drop
            }
        });
        self.relink(Some(link), None)
    }

    /// Expand one value into zero or more. Each yielded value is keyed and
    /// delivered independently, so `flat_map` + [`key_by`](Stream::key_by) is
    /// how one input fans out across several Entities. Same purity and cost
    /// contract as [`map`](Stream::map).
    ///
    /// The yielded values are pushed one at a time under the channel's
    /// `on_full` policy, so a `DropNewest` channel may accept some of a
    /// fan-out and drop the rest — the same shape multi-consumer fan-out
    /// already has.
    pub fn flat_map<U, I, F>(self, f: F) -> Stream<'b, U, K>
    where
        T: Clone,
        U: Send + Sync + 'static,
        I: IntoIterator<Item = U>,
        F: Fn(T) -> I + Send + Sync + 'static,
    {
        let link = erase_transform::<T, _>("flat_map", move |typed: &T, _| {
            EdgeYield::Many(
                f(typed.clone())
                    .into_iter()
                    .map(|v| Arc::new(v) as Arc<dyn Any + Send + Sync>)
                    .collect(),
            )
        });
        self.relink(Some(link), None)
    }

    /// Name the Entity each value is routed to, making the consumer a
    /// dynamic-mode Procedure whose Entities are created on first sight of a
    /// key. This is the only way to declare dynamic keying — the consumer
    /// Procedure carries no keying member at all, so a reader of `channels()`
    /// can see where work fans out.
    ///
    /// Total by signature: it names an Entity, it never declines. Discard
    /// inputs with [`filter`](Stream::filter) before it. It is evaluated on the
    /// **post-transform** value, so it must be the last link before `.into`.
    /// Wiring it onto a Procedure declaring `entity_keys()` (static mode) is a
    /// registration error.
    pub fn key_by<F>(self, key: F) -> Self
    where
        F: Fn(&T) -> String + Send + Sync + 'static,
    {
        self.relink(None, Some(erase::<T, _, _>("key_by", key)))
    }

    /// Override the default `OverflowPolicy` (`DropNothing` for every
    /// channel kind). On an entry channel `DropNothing` parks the shared wire
    /// reader on this operative's saturation (see
    /// `docs/sdk/rust_operative_architecture.md` for the co-tenancy cost);
    /// declare `DropNewest` / `DropOldest` when shedding under saturation is
    /// acceptable and ordering or completeness can be sacrificed for liveness.
    pub fn on_full(self, policy: OverflowPolicy) -> Self {
        self.with_channel(|st, cid| st.channels[cid as usize].overflow_policy = policy);
        self
    }

    /// Override the consumer-side retry budget (total attempts, default 3)
    /// applied before a failing payload is parked to the dead-letter queue.
    pub fn retry_max(self, max_attempts: u32) -> Self {
        self.with_channel(|st, cid| set_retry_max(st, cid, max_attempts));
        self
    }

    /// Override the exponential-backoff base (milliseconds, default 50)
    /// waited out between failed attempts. The wait holds no thread — the
    /// entity becomes eligible again at its due time.
    pub fn retry_backoff_ms(self, base_ms: u64) -> Self {
        self.with_channel(|st, cid| st.channels[cid as usize].retry.backoff_base_ms = base_ms);
        self
    }
}

impl<'b, T: 'static + Send + Sync> Stream<'b, T, NodeTarget> {
    /// Branching sugar: each arm receives a copy of this handle, so
    /// `s.branch([&|b| { b.into::<A>(); }, &|b| { b.into::<B>(); }])` builds
    /// exactly the topology of two `s.into` statements — nothing more. At
    /// least two arms are required (checked at registration); the two
    /// spellings are freely mixable.
    pub fn branch<const N: usize>(self, arms: [&dyn Fn(Stream<'b, T>); N]) -> Self {
        if N < 2 {
            self.builder
                .state
                .borrow_mut()
                .errors
                .push(TopologyIssue::BranchTooFewArms { arms: N });
        }
        for arm in arms {
            arm(self);
        }
        self
    }

    /// Override the auto-derived channel name (the producer node's identity)
    /// everywhere the channel name appears: wire metadata, journals, errors.
    pub fn named(self, name: &'static str) -> Self {
        self.with_channel(|st, cid| st.channels[cid as usize].name = name);
        self
    }

    /// Declare the standard inter-proc byte codec (postcard over `T`'s serde
    /// derives) so this channel's handoff batch is journal-able at the
    /// workflow proc-commit barrier and re-seedable on crash-resume. Required
    /// on every inter-proc channel of a durable structured workflow class.
    pub fn codec(self) -> Self
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        self.with_channel(|st, cid| set_codec::<T>(st, cid));
        self
    }

    /// Declare a custom typed→bytes / bytes→typed codec pair for payload
    /// types without the standard serde derives.
    pub fn codec_with<E, D>(self, encode: E, decode: D) -> Self
    where
        E: Fn(&T) -> Result<Vec<u8>, String> + Send + Sync + 'static,
        D: Fn(Vec<u8>) -> Result<T, String> + Send + Sync + 'static,
    {
        self.with_channel(|st, cid| set_codec_with::<T, E, D>(st, cid, encode, decode));
        self
    }

    /// Opt this channel into output identity: every plugin emit must carry
    /// a natural key via `ctx.emit_keyed(key, value)` and the runtime
    /// stamps `flz.entity` / `flz.seq` / `flz.key` on the wire frame.
    /// Requires a plugin wire (`sink`); `build_topology` rejects a keyed
    /// channel without one.
    pub fn keyed(self) -> Self {
        self.with_channel(|st, cid| st.channels[cid as usize].keyed = true);
        self
    }

    /// Attach this channel to the named plugin sink: every value emitted on it
    /// is serialized once and delivered to the controller's output layer
    /// stamped with the channel name, where the plugins attached to that name
    /// consume it. Several channels may name one sink.
    ///
    /// The handle is returned, so `.into::<S>()` consumer wiring chains after
    /// it and the value then reaches both the consumer mailboxes (typed) and
    /// the sink (as bytes). A channel with no consumers is the plugin-bound
    /// terminal — terminality is the absence of further wiring, not a
    /// different method. [`sealed`](Stream::sealed) states that in the type.
    ///
    /// The serializer runs once per emit regardless of consumer count.
    /// Declaring a second sink on one channel is a registration error
    /// (`TopologyIssue::DuplicatePluginSink`); the first call wins.
    pub fn sink_with<F>(self, plugin_name: &'static str, serializer: F) -> Self
    where
        F: Fn(&T) -> Vec<u8> + Send + Sync + 'static,
    {
        let serializer_erased: crate::channel::PluginSerializeFn =
            erase::<T, _, _>("plugin serializer", serializer);
        self.attach_plugin_sink(plugin_name, serializer_erased)
    }

    /// [`sink_with`](Stream::sink_with) with the default serialization rules:
    /// passthrough for `Vec<u8>`, UTF-8 for `String`, the SDK's default JSON
    /// codec otherwise.
    pub fn sink(self, plugin_name: &'static str) -> Self
    where
        T: serde::Serialize,
    {
        self.attach_plugin_sink(plugin_name, default_plugin_serializer::<T>())
    }

    /// State that nothing follows this channel: the returned [`Channel`]
    /// exposes no builder methods, so a later consumer or transform is a
    /// compile error. Purely type-level — it touches no builder state, so the
    /// topology is identical with or without it.
    pub fn sealed(self) -> Channel<T> {
        Channel::new()
    }

    fn attach_plugin_sink(
        self,
        plugin_name: &'static str,
        serializer: crate::channel::PluginSerializeFn,
    ) -> Self {
        self.with_channel(|st, cid| {
            let entry = &mut st.channels[cid as usize];
            if entry.plugin_sink.is_some() {
                let channel = entry.name;
                st.errors
                    .push(TopologyIssue::DuplicatePluginSink { channel });
                return;
            }
            entry.plugin_sink = Some(PluginSink {
                name: plugin_name,
                serializer,
            });
        });
        self
    }
}
