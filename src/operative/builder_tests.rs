//! `build_topology`'s own tests: the declarations the builder records, the
//! verdict the core answers them with, and the mailbox-capacity precedence
//! the builder resolves before the record is written. The rules themselves
//! are `flz_core::host::check`'s, pinned row by row by
//! `test/fixtures/reference_topologies.txt`.

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

/// The journal codec is derived from the producing procedure's `Output`, so a
/// produced channel carries one whatever the class kind. The bound is
/// unconditional because an operator's per-class persistence override, not the
/// author, decides whether a class journals; nothing in a stream operative's
/// engine path ever calls it (the only readers are the workflow commit and
/// resume paths in `flz_core`).
#[test]
fn a_produced_channel_carries_a_derived_codec() {
    let cb = ChannelBuilder::new();
    cb.input::<String>("in").into::<TierProc>().named("mid");
    let topology = build_topology("derived_codec", cb).expect("topology");
    let mid = topology
        .channels
        .iter()
        .find(|c| &*c.name == "mid")
        .expect("the declared channel exists");

    let codec = mid
        .codec
        .as_ref()
        .expect("a produced channel carries its codec from the moment it exists");
    let value: std::sync::Arc<dyn std::any::Any + Send + Sync> =
        std::sync::Arc::new("handoff".to_string());
    let bytes = (codec.encode)(&*value).expect("the derived codec encodes");
    // The same postcard bytes the declared codec produced, so a journal
    // written before this change re-seeds after it.
    assert_eq!(
        bytes,
        postcard::to_allocvec(&"handoff".to_string()).expect("postcard encodes a String")
    );
    let back = (codec.decode)(bytes).expect("the derived codec decodes");
    assert_eq!(
        back.downcast_ref::<String>()
            .expect("the same type came back"),
        "handoff"
    );
}

/// The other half of the same rule: a plugin-bound terminal is not a handoff.
/// Nothing re-seeds from it, and journaling every plugin payload at every proc
/// barrier would be WAL volume no reader ever asks for — so it carries no
/// codec, exactly as `flz_core::host::build` decides for a byte-passing host.
#[test]
fn a_plugin_bound_terminal_carries_no_codec() {
    let cb = ChannelBuilder::new();
    cb.input::<String>("in")
        .into::<TierProc>()
        .sink_named("out");
    let topology = build_topology("plugin_terminal_codec", cb).expect("topology");
    let out = topology
        .channels
        .iter()
        .find(|c| &*c.name == "out")
        .expect("the declared channel exists");

    assert_eq!(out.kind, crate::channel::ChannelKind::PluginBound);
    assert!(
        out.codec.is_none(),
        "a plugin-bound terminal must ship an empty handoff, not a journaled one"
    );
}

#[test]
fn effectively_once_with_keyed_plugin_channel_builds() {
    let cb = ChannelBuilder::new();
    cb.effectively_once();
    cb.input::<String>("in")
        .into::<TierProc>()
        .keyed()
        .sink_named("out");
    let topology = build_topology("tier_keyed_ok", cb).expect("topology");
    assert!(topology.effectively_once);
}

#[test]
fn effectively_once_rejects_unkeyed_plugin_channel() {
    let cb = ChannelBuilder::new();
    cb.effectively_once();
    cb.input::<String>("in")
        .into::<TierProc>()
        .sink_named("out");
    let err = match build_topology("tier_unkeyed_plugin", cb) {
        Err(e) => e,
        Ok(_) => panic!("unkeyed plugin channel must fail under the tier"),
    };
    assert!(err.errors.iter().any(|e| matches!(
        e,
        TopologyIssue::UnkeyedChannelUnderEffectivelyOnce { channel } if channel == "out"
    )));
}

#[test]
fn effectively_once_rejects_unkeyed_hybrid_sink() {
    let cb = ChannelBuilder::new();
    cb.effectively_once();
    cb.input::<String>("in")
        .into::<TierProc>()
        .sink_named_with("mid", |s: &String| s.as_bytes().to_vec());
    let err = match build_topology("tier_unkeyed_hybrid", cb) {
        Err(e) => e,
        Ok(_) => panic!("unkeyed hybrid sink must fail under the tier"),
    };
    assert!(err.errors.iter().any(|e| matches!(
        e,
        TopologyIssue::UnkeyedChannelUnderEffectivelyOnce { channel } if channel == "mid"
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
        .sink_named("out");
    let topology = build_topology("no_tier_unkeyed_ok", cb).expect("topology");
    assert!(!topology.effectively_once);
}

#[test]
fn override_on_procedure_takes_precedence() {
    // Procedure declared 4096 with MAILBOX_CAPACITY_OVERRIDDEN = true;
    // env var present and parseable. Override wins.
    assert_eq!(resolve_mailbox_capacity(4096, true, Some("2048")), 4096);
}

#[test]
fn env_var_applies_when_no_procedure_override() {
    // No override; env var is parseable. Env wins over the default.
    assert_eq!(resolve_mailbox_capacity(1024, false, Some("2048")), 2048);
}

#[test]
fn env_var_unparseable_falls_through_to_default() {
    // No override; env var present but unparseable. Default kicks in.
    assert_eq!(
        resolve_mailbox_capacity(1024, false, Some("banana")),
        DEFAULT_MAILBOX_CAPACITY
    );
}

#[test]
fn env_var_missing_falls_through_to_default() {
    assert_eq!(
        resolve_mailbox_capacity(1024, false, None),
        DEFAULT_MAILBOX_CAPACITY
    );
}

#[test]
fn an_overridden_zero_wins_over_the_env_var() {
    // Precedence runs to the end: an explicit-but-invalid override is what
    // reaches the record, where the core refuses it by name.
    assert_eq!(resolve_mailbox_capacity(0, true, Some("4096")), 0);
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
        .keyed()
        .sink_named("out");
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
        .keyed()
        .sink_named("out");
    let topology = build_topology("checkpoint_ok", cb).expect("topology");
    assert!(topology.ack_granularity.is_checkpoint());
}

#[test]
fn the_interval_options_reach_the_topology_and_imply_the_granularity() {
    use crate::operative::topology::CheckpointOutputs;

    let cb = ChannelBuilder::new();
    cb.effectively_once();
    cb.checkpoint_every(500);
    cb.outputs_at_checkpoint(50);
    cb.input::<String>("in")
        .into::<TierProc>()
        .keyed()
        .sink_named("out");
    let topology = build_topology("checkpoint_options", cb).expect("topology");

    let policy = topology
        .ack_granularity
        .checkpoint()
        .expect("an interval option implies the checkpoint granularity");
    assert_eq!(policy.every, Some(500));
    assert_eq!(policy.outputs, CheckpointOutputs::AtCheckpoint);
    assert_eq!(
        policy.max_delay_ms,
        Some(50),
        "the delay is a parameter of the option, so it cannot be omitted"
    );
}

#[test]
fn a_bare_checkpoint_ack_holds_no_outputs() {
    use crate::operative::topology::CheckpointOutputs;

    let cb = ChannelBuilder::new();
    cb.effectively_once();
    cb.checkpoint_ack();
    cb.input::<String>("in")
        .into::<TierProc>()
        .keyed()
        .sink_named("out");
    let topology = build_topology("checkpoint_bare", cb).expect("topology");

    let policy = topology.ack_granularity.checkpoint().expect("checkpoint");
    assert_eq!(
        policy.outputs,
        CheckpointOutputs::AsProduced,
        "checkpointing moves the acknowledgement, not the output"
    );
    assert_eq!(policy.every, None);
}

#[test]
fn a_zero_interval_is_refused() {
    let cb = ChannelBuilder::new();
    cb.effectively_once();
    cb.checkpoint_every(0);
    cb.input::<String>("in")
        .into::<TierProc>()
        .keyed()
        .sink_named("out");
    let err = build_topology("checkpoint_zero", cb)
        .err()
        .expect("an interval that closes every zero inputs is not an interval");

    assert!(err
        .errors
        .iter()
        .any(|e| matches!(e, TopologyIssue::CheckpointEveryZero)));
}

#[test]
fn per_input_is_the_default_ack_granularity() {
    let cb = ChannelBuilder::new();
    cb.effectively_once();
    cb.input::<String>("in")
        .into::<TierProc>()
        .keyed()
        .sink_named("out");
    let topology = build_topology("per_input_default", cb).expect("topology");
    assert!(!topology.ack_granularity.is_checkpoint());
}

#[test]
fn a_dropping_policy_under_the_tier_is_refused_naming_the_channel() {
    // Promising effectively-once and asking to shed is a contradiction:
    // a dropped input's offset is advanced past by a later item's
    // watermark, so the loss is permanent under a contract that says
    // nothing is lost.
    for policy in [OverflowPolicy::DropNewest, OverflowPolicy::DropOldest] {
        let cb = ChannelBuilder::new();
        cb.effectively_once();
        cb.input::<String>("in")
            .on_full(policy)
            .into::<TierProc>()
            .keyed()
            .sink_named("out");

        let err = build_topology("tier_sheds", cb)
            .err()
            .expect("a shedding channel under the tier must fail");

        assert!(
            err.errors.iter().any(|e| matches!(
                e,
                TopologyIssue::DroppingPolicyUnderEffectivelyOnce { channel, .. }
                    if channel == "in"
            )),
            "the refusal names the channel: {:?}",
            err.errors
        );
    }
}

#[test]
fn a_dropping_policy_off_the_tier_still_builds() {
    // Shedding is a legitimate intent — it is only the combination that
    // is refused.
    let cb = ChannelBuilder::new();
    cb.input::<String>("in")
        .on_full(OverflowPolicy::DropNewest)
        .into::<TierProc>()
        .sink_named("out");

    build_topology("plain_sheds", cb).expect("a shedding class without the tier builds");
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
        .sink_named("out");

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

    // The refusal names the clock the record spells, not the Rust type.
    assert!(err.errors.iter().any(|e| matches!(
        e,
        TopologyIssue::CorrectionWithoutEventTime { clock_type, .. } if clock_type == "wall"
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
