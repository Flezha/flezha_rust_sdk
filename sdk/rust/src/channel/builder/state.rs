//! The topology under construction: the records the declarations write into,
//! the node-identity and edge-wiring rules, and the callback erasures every
//! typed declaration is stored behind.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use flz_core::host::HostLink;

use crate::channel::{
    ChannelId, ChannelKind, EdgeKeyFn, EdgeTransformFn, EdgeYield, OverflowPolicy, ProcedureId,
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
    /// refuses clock configurations the runtime cannot serve (a
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

    /// The journal codec of everything this node releases, derived from
    /// `S::Output` and installed on the node's output channel the moment that
    /// channel is created. There is no declaration to forget.
    pub output_codec: crate::channel::ChannelCodec,
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
    pub wrap_input: Option<crate::channel::EntryWrapFn>,
    /// Entry channels whose `wrap_input` is a real parser (typed JSON or a
    /// custom `input_with` parser). A failing parse rides the channel's
    /// retry→DLQ ladder; `Vec<u8>`/`String` passthrough entries keep the
    /// pre-change drop-and-log behaviour on invalid input.
    pub entry_parsed: bool,
    /// The channel's plugin egress, populated by `sink(...)` / `sink_with(...)`.
    pub plugin_sink: Option<PluginSink>,
    /// Output-identity opt-in set by `.keyed()`. Valid only on channels
    /// with a plugin wire; `build_topology` refuses a keyed channel that
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
    /// Inter-proc byte codec, derived from the producing procedure's `Output`
    /// type when the channel is created. `None` only on a channel with no
    /// producer (an entry channel); a durable structured workflow's proc
    /// journal reads it at every barrier (`workflow-proc-journal`).
    pub codec: Option<crate::channel::ChannelCodec>,
    /// The authoring record: one entry per fluent sequence declared off this
    /// channel's handle, in the order the author closed them. The consumers,
    /// the kind and the plugin-sink name are DERIVED from these by
    /// `flz_core::host::check`, never declared.
    pub chains: Vec<Vec<HostLink>>,
    /// The closures each chain ends in, under the chain's own index — the half
    /// the record never carries. Attached to the edge the core derives from
    /// that chain.
    pub chain_edges: Vec<(Option<EdgeTransformFn>, Option<EdgeKeyFn>)>,
}

/// A transform/key chain declared on a stream handle but not yet closed by
/// `.into(...)`. Every `.map` / `.filter` / `.flat_map` / `.key_by` pushes a
/// NEW entry composing the previous one, which is what keeps the handles
/// `Copy` (they carry an index, not an `Arc`) and what makes reusing a handle
/// mid-chain branch cleanly instead of aliasing.
pub(crate) struct PendingEdge {
    /// The channel the chain is declared on — the diagnostic's subject.
    pub channel: ChannelId,
    /// The same chain as the record spells it: one link per declaration, in
    /// the order they were written. Closing it appends the terminator.
    pub links: Vec<HostLink>,
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
    /// Merges declared so far; the counter that ties one merge's sources
    /// together across the channels they are declared on.
    pub merge_groups: u32,
    pub effectively_once: bool,
    pub correction: Option<crate::operative::topology::CorrectionPolicy>,
    /// The checkpoint flags exactly as the author set them. The granularity
    /// they imply is the core's answer, never the builder's.
    pub checkpoint_ack: bool,
    pub checkpoint_every: Option<u32>,
    pub checkpoint_outputs: crate::operative::topology::CheckpointOutputs,
    pub checkpoint_max_delay_ms: Option<u64>,
}

impl BuilderState {
    pub(super) fn push_channel(&mut self, name: &'static str, kind: ChannelKind) -> u32 {
        let id = self.channels.len() as u32;
        self.channels.push(ChannelEntry {
            name,
            kind,
            producer: None,
            wrap_input: None,
            entry_parsed: false,
            plugin_sink: None,
            keyed: false,
            overflow_policy: OverflowPolicy::default(),
            retry: crate::channel::RetryPolicy::default(),
            codec: None,
            chains: Vec::new(),
            chain_edges: Vec::new(),
        });
        id
    }

    /// Resolve or create the node for `S` under the node-identity rule: two
    /// unnamed mentions of one class are the same node; an explicit node name
    /// creates/refers to a distinct node whose identity string replaces
    /// `S::name()`. An identity claimed by a *different* declaration is a
    /// collision.
    pub(super) fn resolve_node<S: Procedure>(&mut self, node_name: Option<&'static str>) -> u32 {
        let identity = node_name.unwrap_or_else(S::name);
        let named = node_name.is_some();
        if let Some(&id) = self.procedure_name_to_id.get(identity) {
            let existing = &self.procedures[id as usize];
            if existing.class_id == TypeId::of::<S>() && existing.named == named {
                return id;
            }
            // Two distinct nodes claiming one identity: the record carries both
            // under that name, which is what the collision IS.
            let id = self.procedures.len() as u32;
            self.procedures.push(procedure_entry::<S>(identity, named));
            return id;
        }
        let id = self.procedures.len() as u32;
        self.procedures.push(procedure_entry::<S>(identity, named));
        self.procedure_name_to_id.insert(identity, id);
        id
    }

    /// The node's output channel, created lazily on first use so a leaf node
    /// never carries a phantom channel. Named by the node's identity until a
    /// `.named(...)` override.
    pub(super) fn ensure_output_channel(&mut self, node: u32) -> u32 {
        if let Some(id) = self.procedures[node as usize].output_channel_id {
            return id;
        }
        let name = self.procedures[node as usize].name;
        let codec = self.procedures[node as usize].output_codec.clone();
        let id = self.push_channel(name, ChannelKind::InterProcedure);
        self.channels[id as usize].producer = Some(node);
        // Derived, never declared: a produced channel carries its codec from
        // the moment it exists, so the handoff is journalable by construction.
        self.channels[id as usize].codec = Some(codec);
        self.procedures[node as usize].output_channel_id = Some(id);
        id
    }

    /// Wire `channel_id` into a consumer node (created/resolved by identity),
    /// closing `pending`'s transform chain onto the resulting edge.
    pub(super) fn wire_consumer<S: Procedure>(
        &mut self,
        channel_id: u32,
        pending: Option<u32>,
        node_name: Option<&'static str>,
    ) -> u32 {
        let consumer = self.resolve_node::<S>(node_name);
        self.attach_consumer(channel_id, consumer, pending, None);
        consumer
    }

    /// Record one consumer subscription on a channel, closing `pending`'s
    /// transform/key chain onto it — a `merge::<I>().from(..)` mapping is one
    /// link of that chain. `None` is the bare `.into`, where the channel's
    /// payload already IS the consumer's `Input` and the consumer's Entity
    /// mode is its own. Closing the chain here is what makes it impossible to
    /// wire an edge and forget its closures.
    ///
    /// `merge_group` is the merge this chain closes as one source of, if any;
    /// the core reads the group to judge the merge's arity and its sources.
    pub(super) fn attach_consumer(
        &mut self,
        channel_id: u32,
        consumer: u32,
        pending: Option<u32>,
        merge_group: Option<u32>,
    ) {
        let (mut links, transform, key) = self.close_pending(pending);
        links.push(match merge_group {
            Some(group) => HostLink::Merge {
                procedure: consumer,
                group,
            },
            None => HostLink::Into {
                procedure: consumer,
            },
        });
        self.push_chain(channel_id, links, transform, key);
        let proc = &mut self.procedures[consumer as usize];
        if !proc.input_channel_ids.contains(&channel_id) {
            proc.input_channel_ids.push(channel_id);
        }
    }

    /// Record one link that stands on its own — `.named(...)`, `.sink(...)`,
    /// the `branch(...)` arity — as its own chain, carrying no closures.
    pub(super) fn record_link(&mut self, channel: ChannelId, link: HostLink) {
        self.push_chain(channel, vec![link], None, None);
    }

    /// Record every chain the author never closed. A chain is stored on the
    /// edge it terminates in, so an unclosed tip reaches the record as written
    /// and the core answers `DanglingTransformChain`. Only a tip can dangle —
    /// every earlier link was superseded by the next.
    pub(crate) fn record_open_chains(&mut self) {
        for id in 0..self.pending.len() {
            let chain = &self.pending[id];
            if chain.superseded || chain.consumed {
                continue;
            }
            let (channel, links) = (chain.channel, chain.links.clone());
            self.push_chain(channel, links, None, None);
        }
    }

    /// The next merge's group id.
    pub(super) fn next_merge_group(&mut self) -> u32 {
        let group = self.merge_groups;
        self.merge_groups += 1;
        group
    }

    fn push_chain(
        &mut self,
        channel: ChannelId,
        links: Vec<HostLink>,
        transform: Option<EdgeTransformFn>,
        key: Option<EdgeKeyFn>,
    ) {
        let entry = &mut self.channels[channel as usize];
        entry.chains.push(links);
        entry.chain_edges.push((transform, key));
    }

    /// Extend the chain at `base` (or open one on `channel`) with one more
    /// link, returning the new chain's id. Composing here — rather than storing
    /// a list the runtime walks — is what makes fusion structural: whatever the
    /// author chained, the edge ends up with exactly one closure. The record
    /// keeps the links themselves, because a rule reads them and a closure is
    /// not a fact anything can judge.
    pub(super) fn extend_pending(
        &mut self,
        channel: ChannelId,
        base: Option<u32>,
        link: Option<EdgeTransformFn>,
        key: Option<EdgeKeyFn>,
    ) -> u32 {
        let (mut links, prior_transform, prior_key) = match base {
            Some(id) => {
                self.pending[id as usize].superseded = true;
                let prior = &self.pending[id as usize];
                (
                    prior.links.clone(),
                    prior.transform.clone(),
                    prior.key.clone(),
                )
            }
            None => (Vec::new(), None, None),
        };
        // One call, one link: every caller passes a transform or a key.
        links.push(if key.is_some() {
            HostLink::Key
        } else {
            HostLink::Transform
        });
        let id = self.pending.len() as u32;
        self.pending.push(PendingEdge {
            channel,
            links,
            transform: compose(prior_transform, link),
            key: key.or(prior_key),
            superseded: false,
            consumed: false,
        });
        id
    }

    /// Take a chain's links and closures and mark it closed. `None` (a bare
    /// handle) is the transform-free case every pre-existing topology is.
    fn close_pending(
        &mut self,
        pending: Option<u32>,
    ) -> (Vec<HostLink>, Option<EdgeTransformFn>, Option<EdgeKeyFn>) {
        let Some(id) = pending else {
            return (Vec::new(), None, None);
        };
        let chain = &mut self.pending[id as usize];
        chain.consumed = true;
        (
            chain.links.clone(),
            chain.transform.clone(),
            chain.key.clone(),
        )
    }
}

/// The node record for `S` under the identity it resolved to. The only site
/// that constructs a [`ProcedureEntry`], so every fact a node carries is read
/// off the Procedure's declaration in one place.
fn procedure_entry<S: Procedure>(name: &'static str, named: bool) -> ProcedureEntry {
    let entities_for_erased: EntitiesForFn =
        erase::<S::Input, _, _>("entities_for", S::entities_for);

    let build_entity: EntityBuilder = Box::new(
        |args: BuildEntityArgs<'_>| -> Arc<crate::entity::EntitySlot> {
            crate::operative::dispatcher::build_entity_slot::<S>(args)
        },
    );

    let (serialize_state, deserialize_state) = crate::operative::dispatcher::build_state_fns::<S>();

    ProcedureEntry {
        name,
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
        output_codec: journal_codec::<S::Output>(),
    }
}

/// The journal codec of a typed procedure output: postcard, through the
/// [`JournalValue`](crate::procedure::JournalValue) bound `S::Output` carries,
/// erased onto the channel the way `flz_core` derives one for an erased host's
/// inter-proc channel.
fn journal_codec<T: crate::procedure::JournalValue + Send + Sync + 'static>(
) -> crate::channel::ChannelCodec {
    crate::channel::ChannelCodec {
        encode: Arc::new(move |val: &dyn Any| {
            val.downcast_ref::<T>()
                .ok_or_else(|| {
                    "inter-proc codec: type mismatch (build-time invariant)".to_string()
                })?
                .to_journal_bytes()
        }),
        decode: Arc::new(move |bytes: Vec<u8>| {
            T::from_journal_bytes(&bytes).map(|v| Arc::new(v) as Arc<dyn Any + Send + Sync>)
        }),
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

/// Erase one author callback — `entities_for`, a `key_by`, a hybrid plugin
/// serializer. The downcast is a build-time
/// invariant (the callback is installed by the same wiring that declared the
/// payload type), so `what` names the declaration only for the impossible
/// case. An erasure whose downcast failure is a *reported* error rather than
/// an impossibility — the inter-proc codec — needs a `Result` and stays
/// hand-written.
pub(super) fn erase<T, R, F>(what: &'static str, f: F) -> ErasedFn<R>
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
pub(super) fn erase_transform<T, F>(what: &'static str, f: F) -> EdgeTransformFn
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
pub(super) fn erase_merge_map<T, I, F>(map: F) -> EdgeTransformFn
where
    T: Clone + Send + Sync + 'static,
    I: Send + Sync + 'static,
    F: Fn(T) -> I + Send + Sync + 'static,
{
    erase_transform::<T, _>("merge mapping", move |value: &T, _| {
        EdgeYield::One(Arc::new(map(value.clone())) as Arc<dyn Any + Send + Sync>)
    })
}
