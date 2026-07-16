//! `build_topology` consumes a `ChannelBuilder` (populated by
//! `UnboundedOperative::channels`) and produces a frozen `Arc<OperativeTopology>`.
//!
//! Build-time validation:
//! - reject duplicate channel names
//! - reject channels declared but never wired with `.from`
//! - reject multi-producer (also blocked by the type system)
//! - reject `segment_for` default impl on operations with non-empty
//!   `operation_keys()` per `design.md` Open Question 1 (resolved via the
//!   `SEGMENT_FOR_OVERRIDDEN` const sentinel on the `Operation` trait)

use std::sync::Arc;

use crate::channel::builder::{BuildTopologyError, ChannelBuilder};
use crate::channel::{ChannelDescriptor, ChannelKind, PluginSink};
use crate::operative::topology::{
    AckGranularity, OperationDescriptor, OperativeTopology, SegmentingMode,
};

/// Built-in default per-Segment mailbox capacity used when a Operation does
/// not override `mailbox_capacity()` and `FLEZHA_MAILBOX_CAPACITY` is
/// either unset or unparseable.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

/// Hard upper bound on per-Segment mailbox capacity. ~1M items × ~40 B
/// per slot ≈ 40 MB per Segment is past sanity for any expected
/// workload; values above this fail registration with a named error.
pub const MAX_MAILBOX_CAPACITY: usize = 1_048_576;

/// Environment variable consulted when a Operation does not set
/// `MAILBOX_CAPACITY_OVERRIDDEN = true`. Parsed as `usize`; empty,
/// missing, or unparseable values fall through to
/// [`DEFAULT_MAILBOX_CAPACITY`].
pub const ENV_MAILBOX_CAPACITY: &str = "FLEZHA_MAILBOX_CAPACITY";

#[derive(Debug, thiserror::Error)]
#[error("operative topology validation failed: {errors:?}")]
pub struct TopologyBuildError {
    pub errors: Vec<BuildTopologyError>,
}

pub fn build_topology(
    factory_id: &'static str,
    builder: ChannelBuilder,
) -> Result<Arc<OperativeTopology>, TopologyBuildError> {
    build_topology_with_persistence(factory_id, builder, true)
}

/// Like [`build_topology`] but accepts the `UnboundedOperative::PERSISTENCE_ENABLED`
/// const captured at registration. `build_topology_for::<R>()` is the
/// canonical caller; tests that exercise topology without an UnboundedOperative
/// generic call [`build_topology`] which defaults the const to `true`.
pub fn build_topology_with_persistence(
    factory_id: &'static str,
    builder: ChannelBuilder,
    persistence_enabled_const: bool,
) -> Result<Arc<OperativeTopology>, TopologyBuildError> {
    let ChannelBuilder {
        channels,
        name_to_id,
        operations,
        operation_name_to_id,
        mut errors,
        effectively_once,
        correction,
        ack_granularity,
    } = builder;

    // Correction replays the durable input log against a rewound baseline.
    // Without the tier there is no durable input log to replay — inputs are
    // forwarded, not journaled — so the declaration is meaningless, not merely
    // degraded. (The tier's own `UnkeyedChannelUnderEffectivelyOnce` check
    // already forces every plugin channel keyed, which is correction's other
    // hard prerequisite: a bumped epoch can only overwrite by natural key.)
    if correction.is_some() && !effectively_once {
        errors.push(BuildTopologyError::CorrectionWithoutTier);
    }
    if correction.is_some_and(|c| c.window_ms == 0) {
        errors.push(BuildTopologyError::CorrectionWindowZero);
    }

    // The checkpoint is where the tier's consumed-offset advances; off the
    // tier there is no durable offset to hold back. The mode's other
    // precondition — every plugin channel keyed, so the interval's re-emitted
    // outputs are absorbed by sink dedup — is already enforced for the whole
    // tier by `UnkeyedChannelUnderEffectivelyOnce` below.
    if ack_granularity == AckGranularity::Checkpoint && !effectively_once {
        errors.push(BuildTopologyError::CheckpointAckWithoutTier);
    }

    // Build channel descriptors and validate.
    let mut channel_descs: Vec<ChannelDescriptor> = Vec::with_capacity(channels.len());
    for entry in channels {
        if entry.producer.is_none() && entry.kind != ChannelKind::Entry {
            errors.push(BuildTopologyError::UnwiredChannel { name: entry.name });
        }
        // Identity stamps only exist on plugin frames; a keyed channel
        // must have a plugin wire (pure `to_plugin` or hybrid sink).
        if entry.keyed && entry.kind != ChannelKind::PluginBound && entry.plugin_sink.is_none() {
            errors.push(BuildTopologyError::KeyedChannelWithoutPluginWire {
                channel: entry.name,
            });
        }
        // The effectively-once tier deduplicates by output identity, which
        // only keyed channels carry — an unkeyed plugin wire would silently
        // break the guarantee.
        if effectively_once
            && !entry.keyed
            && (entry.kind == ChannelKind::PluginBound || entry.plugin_sink.is_some())
        {
            errors.push(BuildTopologyError::UnkeyedChannelUnderEffectivelyOnce {
                channel: entry.name,
            });
        }
        let plugin_sink = entry.plugin_sink.map(|sink| PluginSink {
            name: Arc::from(sink.name),
            serializer: sink.serializer,
        });
        channel_descs.push(ChannelDescriptor {
            name: Arc::from(entry.name),
            kind: entry.kind,
            producer: entry.producer,
            consumers: entry.consumers,
            plugin_name: entry.plugin_name.map(Arc::from),
            wrap_input: entry.wrap_input,
            plugin_serializer: entry.plugin_serializer,
            plugin_sink,
            keyed: entry.keyed,
            overflow_policy: entry.overflow_policy,
            retry: entry.retry,
            codec: entry.codec,
        });
    }

    // Build operation descriptors and validate segment declarations.
    let mut operation_descs: Vec<OperationDescriptor> = Vec::with_capacity(operations.len());
    for entry in operations {
        if !entry.segment_keys.is_empty() && !entry.segment_for_overridden {
            errors.push(BuildTopologyError::SegmentForMissing { name: entry.name });
        }
        // A Operation declares static keys OR dynamic operation_key(), never both.
        if entry.operation_key_overridden && !entry.segment_keys.is_empty() {
            errors.push(BuildTopologyError::StaticAndDynamic { name: entry.name });
        }
        // Build-time clock validation: LogicalClock serves the durable
        // change seq, which never advances for a NoChange Operation — fail
        // fast instead of serving a clock frozen at 0.
        if entry.clock_type_name == "LogicalClock" && entry.change_is_nochange {
            errors.push(BuildTopologyError::UnservableClock {
                operation: entry.name,
                clock_type: entry.clock_type_name,
            });
        }
        // Correction orders a replayed slice by event time. An operation whose
        // clock is the wall or the change seq has no event time, so the sort
        // key does not exist and lateness is undefined.
        if correction.is_some() && !entry.clock_is_event_time {
            errors.push(BuildTopologyError::CorrectionWithoutEventTime {
                operation: entry.name,
                clock_type: entry.clock_type_name,
            });
        }
        let mode = match (
            entry.segment_keys.is_empty(),
            entry.operation_key_overridden,
        ) {
            (false, _) => SegmentingMode::Static,
            (true, true) => SegmentingMode::Dynamic,
            (true, false) => SegmentingMode::Single,
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
        operation_descs.push(OperationDescriptor {
            name: Arc::from(entry.name),
            segment_keys: entry.segment_keys,
            segment_for_overridden: entry.segment_for_overridden,
            operation_key_overridden: entry.operation_key_overridden,
            compensate_overridden: entry.compensate_overridden,
            mode,
            idle_ttl: entry.idle_ttl,
            max_segments: entry.max_segments,
            operation_key_erased: entry.operation_key_erased,
            input_channel_id: entry.input_channel_id,
            output_channel_id: entry.output_channel_id,
            input_type_id: entry.input_type_id,
            output_type_id: entry.output_type_id,
            mailbox_capacity: resolved_capacity,
            mailbox_capacity_overridden: entry.mailbox_capacity_overridden,
            build_segment: entry.build_segment,
            segment_for_erased: entry.segment_for_erased,
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
        operations: operation_descs,
        operation_name_to_id,
        factory_id,
        persistence_enabled_const,
        effectively_once,
        correction,
        ack_granularity,
    }))
}

/// Convenience constructor used by `register_unbounded::<R>()`. Calls
/// `R::channels(...)` once and returns the resulting topology, capturing
/// `R::PERSISTENCE_ENABLED` so `UnboundedInstance::new` can resolve the
/// final per-instance flag.
pub fn build_topology_for<R: crate::operative::UnboundedOperative>(
) -> Result<Arc<OperativeTopology>, TopologyBuildError> {
    let mut cb = ChannelBuilder::new();
    R::channels(&mut cb);
    build_topology_with_persistence(R::factory_id(), cb, R::PERSISTENCE_ENABLED)
}

/// Resolve a Operation's mailbox capacity per Decision 6: an explicit
/// override on the Operation wins over `FLEZHA_MAILBOX_CAPACITY`, which wins
/// over the built-in default. Out-of-range values fail registration with
/// the offending Operation's name; an unparseable env var falls through to
/// the default rather than failing (runners tolerate typos in env
/// values during development).
pub(crate) fn resolve_mailbox_capacity(
    operation_name: &'static str,
    declared: usize,
    overridden: bool,
) -> Result<usize, BuildTopologyError> {
    let env_value = std::env::var(ENV_MAILBOX_CAPACITY).ok();
    resolve_mailbox_capacity_inner(operation_name, declared, overridden, env_value.as_deref())
}

/// Pure-function core split out of [`resolve_mailbox_capacity`] so unit
/// tests can exercise the precedence rules without mutating the
/// process-global `FLEZHA_MAILBOX_CAPACITY` env var.
pub(crate) fn resolve_mailbox_capacity_inner(
    operation_name: &'static str,
    declared: usize,
    overridden: bool,
    env_value: Option<&str>,
) -> Result<usize, BuildTopologyError> {
    let resolved = if overridden {
        declared
    } else {
        env_value
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAILBOX_CAPACITY)
    };
    validate_capacity(operation_name, resolved)
}

fn validate_capacity(
    operation_name: &'static str,
    value: usize,
) -> Result<usize, BuildTopologyError> {
    if value == 0 {
        return Err(BuildTopologyError::MailboxCapacityZero {
            operation: operation_name,
        });
    }
    if value > MAX_MAILBOX_CAPACITY {
        return Err(BuildTopologyError::MailboxCapacityTooLarge {
            operation: operation_name,
            value,
        });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::ChannelBuilder;
    use crate::operation::state_handle::StateHandle;
    use crate::operation::{NoChange, Operation, OperationCtx, OperationError, WallClock};
    use crate::operative::topology::BeyondWindowPolicy;
    use crate::segment::restore::RestoreInitPayload;

    #[derive(Default)]
    struct TierOp;
    impl Operation for TierOp {
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
            _: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            Ok(())
        }
    }

    #[test]
    fn effectively_once_with_keyed_plugin_channel_builds() {
        let mut cb = ChannelBuilder::new();
        cb.effectively_once();
        cb.input::<String>("in").into::<TierOp>();
        cb.channel::<String>("out")
            .from::<TierOp>()
            .keyed()
            .to_plugin("kafka");
        let topology = build_topology("tier_keyed_ok", cb).expect("topology");
        assert!(topology.effectively_once);
    }

    #[test]
    fn effectively_once_rejects_unkeyed_plugin_channel() {
        let mut cb = ChannelBuilder::new();
        cb.effectively_once();
        cb.input::<String>("in").into::<TierOp>();
        cb.channel::<String>("out")
            .from::<TierOp>()
            .to_plugin("kafka");
        let err = match build_topology("tier_unkeyed_plugin", cb) {
            Err(e) => e,
            Ok(_) => panic!("unkeyed plugin channel must fail under the tier"),
        };
        assert!(err.errors.iter().any(|e| matches!(
            e,
            BuildTopologyError::UnkeyedChannelUnderEffectivelyOnce { channel: "out" }
        )));
    }

    #[test]
    fn effectively_once_rejects_unkeyed_hybrid_sink() {
        let mut cb = ChannelBuilder::new();
        cb.effectively_once();
        cb.input::<String>("in").into::<TierOp>();
        cb.channel::<String>("mid")
            .from::<TierOp>()
            .also_to_plugin_with("kafka", |s: &String| s.as_bytes().to_vec());
        let err = match build_topology("tier_unkeyed_hybrid", cb) {
            Err(e) => e,
            Ok(_) => panic!("unkeyed hybrid sink must fail under the tier"),
        };
        assert!(err.errors.iter().any(|e| matches!(
            e,
            BuildTopologyError::UnkeyedChannelUnderEffectivelyOnce { channel: "mid" }
        )));
    }

    #[test]
    fn default_topology_is_not_effectively_once() {
        let mut cb = ChannelBuilder::new();
        cb.input::<String>("in").into::<TierOp>();
        let topology = build_topology("tier_default_off", cb).expect("topology");
        assert!(!topology.effectively_once);
    }

    #[test]
    fn non_tier_unkeyed_plugin_channel_still_builds() {
        let mut cb = ChannelBuilder::new();
        cb.input::<String>("in").into::<TierOp>();
        cb.channel::<String>("out")
            .from::<TierOp>()
            .to_plugin("kafka");
        let topology = build_topology("no_tier_unkeyed_ok", cb).expect("topology");
        assert!(!topology.effectively_once);
    }

    #[test]
    fn override_on_operation_takes_precedence() {
        // Operation declared 4096 with MAILBOX_CAPACITY_OVERRIDDEN = true;
        // env var present and parseable. Override wins.
        let cap = resolve_mailbox_capacity_inner("ohlc", 4096, true, Some("2048")).unwrap();
        assert_eq!(cap, 4096);
    }

    #[test]
    fn env_var_applies_when_no_operation_override() {
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
    fn capacity_zero_is_rejected_with_operation_name() {
        // Override path: explicit 0 is rejected.
        let err = resolve_mailbox_capacity_inner("ohlc", 0, true, None).unwrap_err();
        match err {
            BuildTopologyError::MailboxCapacityZero { operation } => assert_eq!(operation, "ohlc"),
            other => panic!("expected MailboxCapacityZero, got {other:?}"),
        }
    }

    #[test]
    fn capacity_above_maximum_is_rejected_with_operation_name() {
        let err = resolve_mailbox_capacity_inner("ohlc", MAX_MAILBOX_CAPACITY + 1, true, None)
            .unwrap_err();
        match err {
            BuildTopologyError::MailboxCapacityTooLarge { operation, value } => {
                assert_eq!(operation, "ohlc");
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
            BuildTopologyError::MailboxCapacityZero { operation: "ohlc" }
        ));
    }

    #[test]
    fn capacity_one_is_accepted() {
        let cap = resolve_mailbox_capacity_inner("tiny", 1, true, None).unwrap();
        assert_eq!(cap, 1);
    }

    #[test]
    fn maximum_capacity_is_accepted() {
        let cap = resolve_mailbox_capacity_inner("max_operation", MAX_MAILBOX_CAPACITY, true, None)
            .unwrap();
        assert_eq!(cap, MAX_MAILBOX_CAPACITY);
    }

    // ---------- Late-input correction: the declaration's prerequisites ----------

    struct TradeTime;
    impl crate::operation::TimeExtractor<String> for TradeTime {
        fn extract(input: &String) -> u64 {
            input.parse().unwrap_or(0)
        }
    }

    #[derive(Default)]
    struct EventTimeOp;
    impl Operation for EventTimeOp {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = crate::operation::EventTimeFrom<TradeTime>;
        type Config = ();
        fn name() -> &'static str {
            "correction_builder_test_op"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            _: String,
            _: StateHandle<'_, Self>,
            _: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            Ok(())
        }
    }

    fn correction_builder<S: Operation<Input = String, Output = String> + Default>(
        window_ms: u64,
        tier: bool,
    ) -> ChannelBuilder {
        let mut cb = ChannelBuilder::new();
        if tier {
            cb.effectively_once();
        }
        cb.correction_window(window_ms);
        cb.input::<String>("in").into::<S>();
        cb.channel::<String>("out")
            .from::<S>()
            .keyed()
            .to_plugin("kafka");
        cb
    }

    #[test]
    fn correction_with_tier_and_event_time_builds() {
        let topology = build_topology(
            "correction_ok",
            correction_builder::<EventTimeOp>(600_000, true),
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
        let mut cb = ChannelBuilder::new();
        cb.effectively_once();
        cb.checkpoint_ack();
        cb.input::<String>("in").into::<TierOp>();
        cb.channel::<String>("out")
            .from::<TierOp>()
            .keyed()
            .to_plugin("kafka");
        let topology = build_topology("checkpoint_ok", cb).expect("topology");
        assert_eq!(topology.ack_granularity, AckGranularity::Checkpoint);
    }

    #[test]
    fn per_input_is_the_default_ack_granularity() {
        let mut cb = ChannelBuilder::new();
        cb.effectively_once();
        cb.input::<String>("in").into::<TierOp>();
        cb.channel::<String>("out")
            .from::<TierOp>()
            .keyed()
            .to_plugin("kafka");
        let topology = build_topology("per_input_default", cb).expect("topology");
        assert_eq!(topology.ack_granularity, AckGranularity::PerInput);
    }

    #[test]
    fn checkpoint_ack_without_the_tier_is_refused() {
        // The checkpoint is where the tier's consumed-offset advances; off the
        // tier there is no durable offset to hold back, and no durable input
        // log to replay the interval from.
        let mut cb = ChannelBuilder::new();
        cb.checkpoint_ack();
        cb.input::<String>("in").into::<TierOp>();
        cb.channel::<String>("out")
            .from::<TierOp>()
            .to_plugin("kafka");

        let err = build_topology("checkpoint_no_tier", cb)
            .err()
            .expect("checkpoint_ack without the tier must fail");

        assert!(err
            .errors
            .iter()
            .any(|e| matches!(e, BuildTopologyError::CheckpointAckWithoutTier)));
    }

    #[test]
    fn correction_without_the_tier_is_refused() {
        // Without the tier there is no durable input log, so there is nothing to
        // replay — the declaration is meaningless, not merely degraded.
        let err = build_topology(
            "correction_no_tier",
            correction_builder::<EventTimeOp>(600_000, false),
        )
        .err()
        .expect("correction without the tier must fail");

        assert!(err
            .errors
            .iter()
            .any(|e| matches!(e, BuildTopologyError::CorrectionWithoutTier)));
    }

    #[test]
    fn correction_without_an_event_time_clock_is_refused() {
        // TierOp declares WallClock: there is no event time to order a replayed
        // slice by, so lateness is not even definable.
        let err = build_topology(
            "correction_wall_clock",
            correction_builder::<TierOp>(600_000, true),
        )
        .err()
        .expect("correction without an event-time clock must fail");

        assert!(err.errors.iter().any(|e| matches!(
            e,
            BuildTopologyError::CorrectionWithoutEventTime {
                clock_type: "WallClock",
                ..
            }
        )));
    }

    #[test]
    fn a_zero_correction_window_is_refused() {
        let err = build_topology(
            "correction_zero",
            correction_builder::<EventTimeOp>(0, true),
        )
        .err()
        .expect("a zero window must fail");

        assert!(err
            .errors
            .iter()
            .any(|e| matches!(e, BuildTopologyError::CorrectionWindowZero)));
    }

    #[test]
    fn the_beyond_window_policy_is_declarable() {
        let mut cb = correction_builder::<EventTimeOp>(600_000, true);
        cb.beyond_window_policy(BeyondWindowPolicy::Drop);
        let topology = build_topology("correction_drop", cb).expect("topology");

        let policy = topology.correction.expect("correction declared");
        assert_eq!(policy.beyond_window, BeyondWindowPolicy::Drop);
        // ...and setting the policy must not clobber the window.
        assert_eq!(policy.window_ms, 600_000);
    }
}
