//! `build_topology` consumes a `ChannelBuilder` (populated by
//! `StreamOperative::channels`) and produces a frozen `Arc<OperativeTopology>`.
//!
//! Build-time validation:
//! - reject duplicate channel names (derived, `.named(...)`-overridden, and
//!   entry names share one per-operative namespace)
//! - enforce the shared graph-shape rule (`sdk-graph-authoring`): one entry
//!   channel, every node reachable from it, no cycles — fan-out and fan-in
//!   are both legal
//! - reject `entities_for` default impl on procedures with non-empty
//!   `entity_keys()` per `design.md` Open Question 1 (resolved via the
//!   `ENTITIES_FOR_OVERRIDDEN` const sentinel on the `Procedure` trait)

use std::collections::HashMap;
use std::sync::Arc;

use crate::channel::builder::{BuilderState, ChannelBuilder, TopologyIssue};
use crate::channel::{ChannelDescriptor, ChannelKind, PluginSink};
use crate::operative::topology::{
    AckGranularity, EntityMode, OperativeTopology, ProcedureDescriptor,
};

/// Built-in default per-Entity mailbox capacity used when a Procedure does
/// not override `mailbox_capacity()` and `FLEZHA_MAILBOX_CAPACITY` is
/// either unset or unparseable.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

/// Hard upper bound on per-Entity mailbox capacity. ~1M items × ~40 B
/// per slot ≈ 40 MB per Entity is past sanity for any expected
/// workload; values above this fail registration with a named error.
pub const MAX_MAILBOX_CAPACITY: usize = 1_048_576;

/// Environment variable consulted when a Procedure does not set
/// `MAILBOX_CAPACITY_OVERRIDDEN = true`. Parsed as `usize`; empty,
/// missing, or unparseable values fall through to
/// [`DEFAULT_MAILBOX_CAPACITY`].
pub const ENV_MAILBOX_CAPACITY: &str = "FLEZHA_MAILBOX_CAPACITY";

#[derive(Debug, thiserror::Error)]
#[error("operative topology validation failed: {errors:?}")]
pub struct TopologyBuildError {
    pub errors: Vec<TopologyIssue>,
}

/// Result of the shared graph-shape walk (`sdk-graph-authoring`): Kahn
/// topological order over the procedure nodes plus reachability from the
/// entry. One walk serves both the stream validation and the workflow
/// `ProcDag` construction — the shape rule is one rule, so its mechanism is one
/// mechanism.
pub(crate) struct GraphOrder {
    /// Node ids in Kahn topological order (FIFO — for a tree this is exactly
    /// BFS from the root, so workflow `proc_idx` numbering stays stable).
    pub order: Vec<crate::channel::ProcedureId>,
    /// Node id → position in `order`; `usize::MAX` for a node on a cycle.
    pub pos: Vec<usize>,
    /// First node unreachable from the roots, if any (meaningful only when
    /// the graph is acyclic — a cycle already explains unreachability).
    pub unreachable: Option<crate::channel::ProcedureId>,
    /// First node Kahn left over — on (or strictly downstream of) a cycle.
    pub on_cycle: Option<crate::channel::ProcedureId>,
}

/// Walk `n` nodes over `edges` (producer → consumer pairs), rooted at `roots`
/// (the entry channel's consumers). Merge mappings are irrelevant to shape, so
/// an edge is a pair of procedure ids.
pub(crate) fn graph_order(
    n: usize,
    edges: &[(crate::channel::ProcedureId, crate::channel::ProcedureId)],
    roots: &[crate::channel::ProcedureId],
) -> GraphOrder {
    let mut adjacency: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut indegree = vec![0usize; n];
    for &(producer, consumer) in edges {
        adjacency[producer as usize].push(consumer as usize);
        indegree[consumer as usize] += 1;
    }

    let mut reachable = vec![false; n];
    let mut stack: Vec<usize> = roots.iter().map(|&r| r as usize).collect();
    while let Some(node) = stack.pop() {
        if std::mem::replace(&mut reachable[node], true) {
            continue;
        }
        stack.extend(adjacency[node].iter().copied());
    }
    let unreachable = (0..n)
        .find(|&i| !reachable[i])
        .map(|i| i as crate::channel::ProcedureId);

    let mut queue: std::collections::VecDeque<usize> =
        (0..n).filter(|&i| indegree[i] == 0).collect();
    let mut order = Vec::with_capacity(n);
    let mut pos = vec![usize::MAX; n];
    while let Some(node) = queue.pop_front() {
        pos[node] = order.len();
        order.push(node as crate::channel::ProcedureId);
        for &child in &adjacency[node] {
            indegree[child] -= 1;
            if indegree[child] == 0 {
                queue.push_back(child);
            }
        }
    }
    let on_cycle = (0..n)
        .find(|&i| pos[i] == usize::MAX)
        .map(|i| i as crate::channel::ProcedureId);

    GraphOrder {
        order,
        pos,
        unreachable,
        on_cycle,
    }
}

pub fn build_topology(
    factory_id: &'static str,
    builder: ChannelBuilder,
) -> Result<Arc<OperativeTopology>, TopologyBuildError> {
    build_topology_with_persistence(factory_id, builder, true)
}

/// Like [`build_topology`] but accepts the `StreamOperative::PERSISTENCE_ENABLED`
/// const captured at registration. `build_topology_for::<R>()` is the
/// canonical caller; tests that exercise topology without a StreamOperative
/// generic call [`build_topology`] which defaults the const to `true`.
pub fn build_topology_with_persistence(
    factory_id: &'static str,
    builder: ChannelBuilder,
    persistence_enabled_const: bool,
) -> Result<Arc<OperativeTopology>, TopologyBuildError> {
    let BuilderState {
        channels,
        procedures,
        procedure_name_to_id,
        pending,
        mut errors,
        effectively_once,
        correction,
        ack_granularity,
    } = builder.into_state();

    // A transform chain is stored on the consumer edge it terminates in, so
    // one that no `.into(...)` ever closed would silently do nothing. Only a
    // chain's tip can dangle — every earlier link was superseded by the next.
    for chain in &pending {
        if !chain.superseded && !chain.consumed {
            errors.push(TopologyIssue::DanglingTransformChain {
                channel: channels[chain.channel as usize].name,
            });
        }
    }

    // One per-operative channel namespace: derived names (producer node
    // identity), `.named(...)` overrides, and entry names all collide here.
    let mut name_to_id: HashMap<&'static str, u32> = HashMap::new();
    for (id, entry) in channels.iter().enumerate() {
        if name_to_id.insert(entry.name, id as u32).is_some() {
            errors.push(TopologyIssue::DuplicateChannel(entry.name));
        }
    }

    // Shared graph-shape rule (`sdk-graph-authoring`): one OR MORE entry
    // channels feed the graph, every node reachable from some entry, no
    // cycles. Multiple entries let one operative ingest several external
    // sources (e.g. a market pipeline consuming a trade feed and an
    // order-book feed on distinct channels); the controller routes each frame
    // to its entry by `metadata["channel"]`, falling back to the first entry
    // when unset. Fan-out and fan-in are both legal; at-most-one output
    // channel per node holds by construction of the stream-handle builder.
    if !procedures.is_empty() {
        let edges: Vec<_> = channels
            .iter()
            .filter_map(|c| c.producer.map(|p| (p, c)))
            .flat_map(|(p, c)| c.consumers.iter().map(move |e| (p, e.procedure)))
            .collect();
        let roots: Vec<_> = channels
            .iter()
            .filter(|c| c.kind == ChannelKind::Entry)
            .flat_map(|c| c.consumers.iter().map(|e| e.procedure))
            .collect();
        let shape = graph_order(procedures.len(), &edges, &roots);
        if let Some(node) = shape.on_cycle {
            errors.push(TopologyIssue::Cycle {
                procedure: procedures[node as usize].name,
            });
        } else if let Some(node) = shape.unreachable {
            // Defensive: the stream-handle builder only creates nodes by
            // wiring from the entry, so an unreachable acyclic node cannot
            // normally arise. A cycle already explains unreachability.
            errors.push(TopologyIssue::UnreachableProcedure {
                procedure: procedures[node as usize].name,
            });
        }
    }

    // Correction replays the durable input log against a rewound baseline.
    // Without the tier there is no durable input log to replay — inputs are
    // forwarded, not journaled — so the declaration is meaningless, not merely
    // degraded. (The tier's own `UnkeyedChannelUnderEffectivelyOnce` check
    // already forces every plugin channel keyed, which is correction's other
    // hard prerequisite: a bumped epoch can only overwrite by natural key.)
    if correction.is_some() && !effectively_once {
        errors.push(TopologyIssue::CorrectionWithoutTier);
    }
    if correction.is_some_and(|c| c.window_ms == 0) {
        errors.push(TopologyIssue::CorrectionWindowZero);
    }

    // The checkpoint is where the tier's consumed-offset advances; off the
    // tier there is no durable offset to hold back. The mode's other
    // precondition — every plugin channel keyed, so the interval's re-emitted
    // outputs are absorbed by sink dedup — is already enforced for the whole
    // tier by `UnkeyedChannelUnderEffectivelyOnce` below.
    if ack_granularity == AckGranularity::Checkpoint && !effectively_once {
        errors.push(TopologyIssue::CheckpointAckWithoutTier);
    }

    // Entity mode is read off the WIRING now: a procedure reached by an edge
    // carrying a key function is dynamic. Collected before the descriptors are
    // built because that loop consumes the consumer edges.
    let keyed_consumers: std::collections::HashSet<crate::channel::ProcedureId> = channels
        .iter()
        .flat_map(|entry| entry.consumers.iter())
        .filter(|edge| edge.key.is_some())
        .map(|edge| edge.procedure)
        .collect();
    for entry in &channels {
        for edge in &entry.consumers {
            let procedure = &procedures[edge.procedure as usize];
            // Static keying routes by `entities_for` and may name several
            // Entities; edge keying names exactly one. Declaring both is a
            // contradiction, not a precedence question.
            if edge.key.is_some() && !procedure.entity_keys.is_empty() {
                errors.push(TopologyIssue::KeyByOnUnkeyableTarget {
                    channel: entry.name,
                    procedure: procedure.name,
                });
            }
            // The key function is total, so a dynamic-mode procedure has no
            // keyless-drop path — every edge into one must carry a key.
            if edge.key.is_none() && keyed_consumers.contains(&edge.procedure) {
                errors.push(TopologyIssue::UnkeyedEdgeIntoDynamicProcedure {
                    channel: entry.name,
                    procedure: procedure.name,
                });
            }
        }
    }

    // Build channel descriptors and validate.
    let mut channel_descs: Vec<ChannelDescriptor> = Vec::with_capacity(channels.len());
    for entry in channels {
        // Identity stamps only exist on plugin frames; a keyed channel
        // must have a plugin wire.
        if entry.keyed && entry.plugin_sink.is_none() {
            errors.push(TopologyIssue::KeyedChannelWithoutPluginWire {
                channel: entry.name,
            });
        }
        // The effectively-once tier deduplicates by output identity, which
        // only keyed channels carry — an unkeyed plugin wire would silently
        // break the guarantee.
        if effectively_once && !entry.keyed && entry.plugin_sink.is_some() {
            errors.push(TopologyIssue::UnkeyedChannelUnderEffectivelyOnce {
                channel: entry.name,
            });
        }
        // Terminality is derived from the wiring, not from which method the
        // author called: a sink with nothing consuming behind it IS the
        // plugin-bound terminal. Deriving it here is what keeps `.sink(...)`
        // one method and makes a sink declared alongside consumers hybrid by
        // construction rather than by ordering.
        let kind = if entry.plugin_sink.is_some() && entry.consumers.is_empty() {
            ChannelKind::PluginBound
        } else {
            entry.kind
        };
        let plugin_sink = entry.plugin_sink.map(|sink| PluginSink {
            name: Arc::from(sink.name),
            serializer: sink.serializer,
        });
        channel_descs.push(ChannelDescriptor {
            name: Arc::from(entry.name),
            kind,
            producer: entry.producer,
            consumers: entry.consumers,
            wrap_input: entry.wrap_input,
            entry_parsed: entry.entry_parsed,
            plugin_sink,
            keyed: entry.keyed,
            overflow_policy: entry.overflow_policy,
            retry: entry.retry,
            codec: entry.codec,
            entry_observation: flz_core::channel::entry_observation_for(kind, entry.name),
        });
    }

    // Build procedure descriptors and validate entity declarations.
    let mut procedure_descs: Vec<ProcedureDescriptor> = Vec::with_capacity(procedures.len());
    for (id, entry) in procedures.into_iter().enumerate() {
        if !entry.entity_keys.is_empty() && !entry.entities_for_overridden {
            errors.push(TopologyIssue::EntitiesForMissing { name: entry.name });
        }
        // Build-time clock validation: LogicalClock serves the durable
        // change seq, which never advances for a NoChange Procedure — fail
        // fast instead of serving a clock frozen at 0.
        if entry.clock_type_name == "LogicalClock" && entry.change_is_nochange {
            errors.push(TopologyIssue::UnservableClock {
                procedure: entry.name,
                clock_type: entry.clock_type_name,
            });
        }
        // Correction orders a replayed slice by event time. A procedure whose
        // clock is the wall or the change seq has no event time, so the sort
        // key does not exist and lateness is undefined.
        if correction.is_some() && !entry.clock_is_event_time {
            errors.push(TopologyIssue::CorrectionWithoutEventTime {
                procedure: entry.name,
                clock_type: entry.clock_type_name,
            });
        }
        let mode = match (
            entry.entity_keys.is_empty(),
            keyed_consumers.contains(&(id as crate::channel::ProcedureId)),
        ) {
            (false, _) => EntityMode::Static,
            (true, true) => EntityMode::Dynamic,
            (true, false) => EntityMode::Single,
        };
        let resolved_capacity = match resolve_mailbox_capacity(
            entry.name,
            entry.mailbox_capacity,
            entry.mailbox_capacity_overridden,
        ) {
            Ok(cap) => cap,
            Err(err) => {
                errors.push(err);
                // Stash a placeholder so the descriptor list keeps the
                // same indexing; the Err path returns before the topology
                // is published, so the placeholder is never read.
                DEFAULT_MAILBOX_CAPACITY
            }
        };
        procedure_descs.push(ProcedureDescriptor {
            host_facts: None,
            name: Arc::from(entry.name),
            entity_keys: entry.entity_keys,
            entities_for_overridden: entry.entities_for_overridden,
            compensate_overridden: entry.compensate_overridden,
            mode,
            idle_ttl: entry.idle_ttl,
            max_entities: entry.max_entities,
            input_channel_ids: entry.input_channel_ids,
            output_channel_id: entry.output_channel_id,
            input_type_id: entry.input_type_id,
            output_type_id: entry.output_type_id,
            mailbox_capacity: resolved_capacity,
            mailbox_capacity_overridden: entry.mailbox_capacity_overridden,
            compact_after_bytes: entry.compact_after_bytes,
            build_entity: entry.build_entity,
            entities_for_erased: entry.entities_for_erased,
            serialize_state: entry.serialize_state,
            deserialize_state: entry.deserialize_state,
        });
    }

    if !errors.is_empty() {
        return Err(TopologyBuildError { errors });
    }

    Ok(Arc::new(OperativeTopology {
        channels: channel_descs,
        name_to_id,
        procedures: procedure_descs,
        procedure_name_to_id,
        factory_id,
        persistence_enabled_const,
        effectively_once,
        correction,
        ack_granularity,
    }))
}

/// Convenience constructor used by `define_stream::<R>()`. Calls
/// `R::channels(...)` once and returns the resulting topology, capturing
/// `R::PERSISTENCE_ENABLED` so `StreamInstance::new` can resolve the
/// final per-instance flag.
pub fn build_topology_for<R: crate::operative::StreamOperative>(
) -> Result<Arc<OperativeTopology>, TopologyBuildError> {
    let cb = ChannelBuilder::new();
    R::channels(&cb);
    build_topology_with_persistence(R::factory_id(), cb, R::PERSISTENCE_ENABLED)
}

/// Resolve a Procedure's mailbox capacity per Decision 6: an explicit
/// override on the Procedure wins over `FLEZHA_MAILBOX_CAPACITY`, which wins
/// over the built-in default. Out-of-range values fail registration with
/// the offending Procedure's name; an unparseable env var falls through to
/// the default rather than failing (runners tolerate typos in env
/// values during development).
pub(crate) fn resolve_mailbox_capacity(
    procedure_name: &'static str,
    declared: usize,
    overridden: bool,
) -> Result<usize, TopologyIssue> {
    let env_value = std::env::var(ENV_MAILBOX_CAPACITY).ok();
    resolve_mailbox_capacity_inner(procedure_name, declared, overridden, env_value.as_deref())
}

/// Pure-function core split out of [`resolve_mailbox_capacity`] so unit
/// tests can exercise the precedence rules without mutating the
/// process-global `FLEZHA_MAILBOX_CAPACITY` env var.
pub(crate) fn resolve_mailbox_capacity_inner(
    procedure_name: &'static str,
    declared: usize,
    overridden: bool,
    env_value: Option<&str>,
) -> Result<usize, TopologyIssue> {
    let resolved = if overridden {
        declared
    } else {
        env_value
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAILBOX_CAPACITY)
    };
    validate_capacity(procedure_name, resolved)
}

fn validate_capacity(procedure_name: &'static str, value: usize) -> Result<usize, TopologyIssue> {
    if value == 0 {
        return Err(TopologyIssue::MailboxCapacityZero {
            procedure: procedure_name,
        });
    }
    if value > MAX_MAILBOX_CAPACITY {
        return Err(TopologyIssue::MailboxCapacityTooLarge {
            procedure: procedure_name,
            value,
        });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::ChannelBuilder;
    use crate::entity::restore::RestoreInitPayload;
    use crate::operative::topology::BeyondWindowPolicy;
    use crate::procedure::state_handle::StateHandle;
    use crate::procedure::{NoChange, Procedure, ProcedureCtx, ProcedureError, WallClock};

    #[derive(Default)]
    struct TierProc;
    impl Procedure for TierProc {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "tier_builder_test_op"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            _: String,
            _: StateHandle<'_, Self>,
            _: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            Ok(())
        }
    }

    #[test]
    fn effectively_once_with_keyed_plugin_channel_builds() {
        let cb = ChannelBuilder::new();
        cb.effectively_once();
        cb.input::<String>("in")
            .into::<TierProc>()
            .named("out")
            .keyed()
            .sink("kafka");
        let topology = build_topology("tier_keyed_ok", cb).expect("topology");
        assert!(topology.effectively_once);
    }

    #[test]
    fn effectively_once_rejects_unkeyed_plugin_channel() {
        let cb = ChannelBuilder::new();
        cb.effectively_once();
        cb.input::<String>("in")
            .into::<TierProc>()
            .named("out")
            .sink("kafka");
        let err = match build_topology("tier_unkeyed_plugin", cb) {
            Err(e) => e,
            Ok(_) => panic!("unkeyed plugin channel must fail under the tier"),
        };
        assert!(err.errors.iter().any(|e| matches!(
            e,
            TopologyIssue::UnkeyedChannelUnderEffectivelyOnce { channel: "out" }
        )));
    }

    #[test]
    fn effectively_once_rejects_unkeyed_hybrid_sink() {
        let cb = ChannelBuilder::new();
        cb.effectively_once();
        cb.input::<String>("in")
            .into::<TierProc>()
            .named("mid")
            .sink_with("kafka", |s: &String| s.as_bytes().to_vec());
        let err = match build_topology("tier_unkeyed_hybrid", cb) {
            Err(e) => e,
            Ok(_) => panic!("unkeyed hybrid sink must fail under the tier"),
        };
        assert!(err.errors.iter().any(|e| matches!(
            e,
            TopologyIssue::UnkeyedChannelUnderEffectivelyOnce { channel: "mid" }
        )));
    }

    #[test]
    fn default_topology_is_not_effectively_once() {
        let cb = ChannelBuilder::new();
        cb.input::<String>("in").into::<TierProc>();
        let topology = build_topology("tier_default_off", cb).expect("topology");
        assert!(!topology.effectively_once);
    }

    #[test]
    fn non_tier_unkeyed_plugin_channel_still_builds() {
        let cb = ChannelBuilder::new();
        cb.input::<String>("in")
            .into::<TierProc>()
            .named("out")
            .sink("kafka");
        let topology = build_topology("no_tier_unkeyed_ok", cb).expect("topology");
        assert!(!topology.effectively_once);
    }

    #[test]
    fn override_on_procedure_takes_precedence() {
        // Procedure declared 4096 with MAILBOX_CAPACITY_OVERRIDDEN = true;
        // env var present and parseable. Override wins.
        let cap = resolve_mailbox_capacity_inner("ohlc", 4096, true, Some("2048")).unwrap();
        assert_eq!(cap, 4096);
    }

    #[test]
    fn env_var_applies_when_no_procedure_override() {
        // No override; env var is parseable. Env wins over the default.
        let cap = resolve_mailbox_capacity_inner("ohlc", 1024, false, Some("2048")).unwrap();
        assert_eq!(cap, 2048);
    }

    #[test]
    fn env_var_unparseable_falls_through_to_default() {
        // No override; env var present but unparseable. Default kicks in.
        let cap = resolve_mailbox_capacity_inner("ohlc", 1024, false, Some("banana")).unwrap();
        assert_eq!(cap, DEFAULT_MAILBOX_CAPACITY);
    }

    #[test]
    fn env_var_missing_falls_through_to_default() {
        let cap = resolve_mailbox_capacity_inner("ohlc", 1024, false, None).unwrap();
        assert_eq!(cap, DEFAULT_MAILBOX_CAPACITY);
    }

    #[test]
    fn capacity_zero_is_rejected_with_procedure_name() {
        // Override path: explicit 0 is rejected.
        let err = resolve_mailbox_capacity_inner("ohlc", 0, true, None).unwrap_err();
        match err {
            TopologyIssue::MailboxCapacityZero { procedure } => assert_eq!(procedure, "ohlc"),
            other => panic!("expected MailboxCapacityZero, got {other:?}"),
        }
    }

    #[test]
    fn capacity_above_maximum_is_rejected_with_procedure_name() {
        let err = resolve_mailbox_capacity_inner("ohlc", MAX_MAILBOX_CAPACITY + 1, true, None)
            .unwrap_err();
        match err {
            TopologyIssue::MailboxCapacityTooLarge { procedure, value } => {
                assert_eq!(procedure, "ohlc");
                assert_eq!(value, MAX_MAILBOX_CAPACITY + 1);
            }
            other => panic!("expected MailboxCapacityTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn override_zero_overrides_env_var_and_fails() {
        // Even if env is valid, an explicit-but-invalid override fails
        // (override-flag-true wins precedence even into validation).
        let err = resolve_mailbox_capacity_inner("ohlc", 0, true, Some("4096")).unwrap_err();
        assert!(matches!(
            err,
            TopologyIssue::MailboxCapacityZero { procedure: "ohlc" }
        ));
    }

    #[test]
    fn capacity_one_is_accepted() {
        let cap = resolve_mailbox_capacity_inner("tiny", 1, true, None).unwrap();
        assert_eq!(cap, 1);
    }

    #[test]
    fn maximum_capacity_is_accepted() {
        let cap = resolve_mailbox_capacity_inner("max_procedure", MAX_MAILBOX_CAPACITY, true, None)
            .unwrap();
        assert_eq!(cap, MAX_MAILBOX_CAPACITY);
    }

    // ---------- Late-input correction: the declaration's prerequisites ----------

    struct TradeTime;
    impl crate::procedure::TimeExtractor<String> for TradeTime {
        fn extract(input: &String) -> u64 {
            input.parse().unwrap_or(0)
        }
    }

    #[derive(Default)]
    struct EventTimeProc;
    impl Procedure for EventTimeProc {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = crate::procedure::EventTimeFrom<TradeTime>;
        type Config = ();
        fn name() -> &'static str {
            "correction_builder_test_op"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            _: String,
            _: StateHandle<'_, Self>,
            _: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            Ok(())
        }
    }

    fn correction_builder<S: Procedure<Input = String, Output = String> + Default>(
        window_ms: u64,
        tier: bool,
    ) -> ChannelBuilder {
        let cb = ChannelBuilder::new();
        if tier {
            cb.effectively_once();
        }
        cb.correction_window(window_ms);
        cb.input::<String>("in")
            .into::<S>()
            .named("out")
            .keyed()
            .sink("kafka");
        cb
    }

    #[test]
    fn correction_with_tier_and_event_time_builds() {
        let topology = build_topology(
            "correction_ok",
            correction_builder::<EventTimeProc>(600_000, true),
        )
        .expect("topology");

        let policy = topology.correction.expect("correction declared");
        assert_eq!(policy.window_ms, 600_000);
        // The default must be the engine's existing behaviour: declaring a
        // window never silently starts discarding data.
        assert_eq!(policy.beyond_window, BeyondWindowPolicy::ProcessAsArrival);
    }

    #[test]
    fn checkpoint_ack_with_the_tier_builds() {
        let cb = ChannelBuilder::new();
        cb.effectively_once();
        cb.checkpoint_ack();
        cb.input::<String>("in")
            .into::<TierProc>()
            .named("out")
            .keyed()
            .sink("kafka");
        let topology = build_topology("checkpoint_ok", cb).expect("topology");
        assert_eq!(topology.ack_granularity, AckGranularity::Checkpoint);
    }

    #[test]
    fn per_input_is_the_default_ack_granularity() {
        let cb = ChannelBuilder::new();
        cb.effectively_once();
        cb.input::<String>("in")
            .into::<TierProc>()
            .named("out")
            .keyed()
            .sink("kafka");
        let topology = build_topology("per_input_default", cb).expect("topology");
        assert_eq!(topology.ack_granularity, AckGranularity::PerInput);
    }

    #[test]
    fn checkpoint_ack_without_the_tier_is_refused() {
        // The checkpoint is where the tier's consumed-offset advances; off the
        // tier there is no durable offset to hold back, and no durable input
        // log to replay the interval from.
        let cb = ChannelBuilder::new();
        cb.checkpoint_ack();
        cb.input::<String>("in")
            .into::<TierProc>()
            .named("out")
            .sink("kafka");

        let err = build_topology("checkpoint_no_tier", cb)
            .err()
            .expect("checkpoint_ack without the tier must fail");

        assert!(err
            .errors
            .iter()
            .any(|e| matches!(e, TopologyIssue::CheckpointAckWithoutTier)));
    }

    #[test]
    fn correction_without_the_tier_is_refused() {
        // Without the tier there is no durable input log, so there is nothing to
        // replay — the declaration is meaningless, not merely degraded.
        let err = build_topology(
            "correction_no_tier",
            correction_builder::<EventTimeProc>(600_000, false),
        )
        .err()
        .expect("correction without the tier must fail");

        assert!(err
            .errors
            .iter()
            .any(|e| matches!(e, TopologyIssue::CorrectionWithoutTier)));
    }

    #[test]
    fn correction_without_an_event_time_clock_is_refused() {
        // TierProc declares WallClock: there is no event time to order a replayed
        // slice by, so lateness is not even definable.
        let err = build_topology(
            "correction_wall_clock",
            correction_builder::<TierProc>(600_000, true),
        )
        .err()
        .expect("correction without an event-time clock must fail");

        assert!(err.errors.iter().any(|e| matches!(
            e,
            TopologyIssue::CorrectionWithoutEventTime {
                clock_type: "WallClock",
                ..
            }
        )));
    }

    #[test]
    fn a_zero_correction_window_is_refused() {
        let err = build_topology(
            "correction_zero",
            correction_builder::<EventTimeProc>(0, true),
        )
        .err()
        .expect("a zero window must fail");

        assert!(err
            .errors
            .iter()
            .any(|e| matches!(e, TopologyIssue::CorrectionWindowZero)));
    }

    #[test]
    fn the_beyond_window_policy_is_declarable() {
        let cb = correction_builder::<EventTimeProc>(600_000, true);
        cb.beyond_window_policy(BeyondWindowPolicy::Drop);
        let topology = build_topology("correction_drop", cb).expect("topology");

        let policy = topology.correction.expect("correction declared");
        assert_eq!(policy.beyond_window, BeyondWindowPolicy::Drop);
        // ...and setting the policy must not clobber the window.
        assert_eq!(policy.window_ms, 600_000);
    }
}
