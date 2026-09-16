//! Section 4.4: harness self-test.
//!
//! Defines a trivial `CounterProcedure` with `Change = u32` whose
//! `apply_change` adds the change to the counter. Asserts that
//! `replay_equals` succeeds for the deterministic procedure, then injects a
//! non-deterministic mutation in a sibling Procedure and asserts
//! `replay_equals` returns `Err(ReplayMismatch)`.

use serde::{Deserialize, Serialize};

use crate::entity::restore::RestoreInitPayload;
use crate::procedure::change::Change;
use crate::procedure::clock::WallClock;
use crate::procedure::ctx::ProcedureCtx;
use crate::procedure::error::ProcedureError;
use crate::procedure::state_handle::StateHandle;
use crate::procedure::Procedure;
use crate::testing::ProcedureTestHarness;

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct CounterState {
    sum: u64,
}

#[derive(Default)]
struct CounterProcedure;

impl Change for u32 {}

impl Procedure for CounterProcedure {
    type Input = u32;
    type Output = ();
    type State = CounterState;
    type Change = u32;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "counter_procedure"
    }

    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> CounterState {
        CounterState::default()
    }

    fn apply_change(state: &mut CounterState, change: &u32) {
        state.sum = state.sum.saturating_add(*change as u64);
    }

    fn run(
        &mut self,
        input: u32,
        mut state: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(input);
        Ok(())
    }
}

#[test]
fn deterministic_procedure_replay_equals_succeeds() {
    let mut harness = ProcedureTestHarness::<CounterProcedure>::new();
    harness.apply_inputs(vec![1u32, 2, 3, 4]);
    assert_eq!(harness.state().sum, 10);
    harness
        .replay_equals()
        .expect("deterministic replay must equal");
}

#[test]
fn expect_changes_chains() {
    let mut harness = ProcedureTestHarness::<CounterProcedure>::new();
    harness.apply_inputs(vec![5u32, 7]);
    harness
        .expect_changes(&[5, 7])
        .expect_state(&CounterState { sum: 12 });
}

// -- Non-deterministic procedure: apply_change reads SystemTime::now() --

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct NoisyState {
    seed: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct NoisyChange;
impl Change for NoisyChange {}

#[derive(Default)]
struct NoisyProcedure;

impl Procedure for NoisyProcedure {
    type Input = ();
    type Output = ();
    type State = NoisyState;
    type Change = NoisyChange;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "noisy_procedure"
    }

    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> NoisyState {
        NoisyState::default()
    }

    fn apply_change(state: &mut NoisyState, _change: &NoisyChange) {
        // Non-deterministic: reads SystemTime::now(). The first
        // application records a non-zero state; the replay records a
        // different value because clock advances.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(0);
        state.seed = state.seed.wrapping_add(nanos);
    }

    fn run(
        &mut self,
        _: (),
        mut state: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(NoisyChange);
        Ok(())
    }
}

#[test]
fn non_deterministic_procedure_replay_equals_fails() {
    let mut harness = ProcedureTestHarness::<NoisyProcedure>::new();
    harness.apply_inputs(vec![(); 4]);
    // The replay's `SystemTime::now()` calls must observe a clock that has
    // moved since the original run.
    let recorded_at = std::time::SystemTime::now();
    while std::time::SystemTime::now() == recorded_at {
        std::hint::spin_loop();
    }
    let result = harness.replay_equals();
    assert!(
        result.is_err(),
        "non-deterministic apply_change should fail replay_equals"
    );
}

// -- Ctx-mediated clocks through the harness --

/// Procedure over `CounterState` with `Change = u32` (`apply_change`
/// sums); I/O types, Clock, and run body vary per test.
macro_rules! clock_procedure {
    ($proc:ident, $name:literal, $input:ty => $output:ty, clock $clock:ty,
     |$inp:tt, $st:tt, $ctx:tt| $body:block) => {
        #[derive(Default)]
        struct $proc;
        impl Procedure for $proc {
            type Input = $input;
            type Output = $output;
            type State = CounterState;
            type Change = u32;
            type Clock = $clock;
            type Config = ();
            fn name() -> &'static str {
                $name
            }
            fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> CounterState {
                CounterState::default()
            }
            fn apply_change(state: &mut CounterState, change: &u32) {
                state.sum += *change as u64;
            }
            fn run(
                &mut self,
                $inp: $input,
                #[allow(unused_mut)] mut $st: StateHandle<'_, Self>,
                $ctx: &mut ProcedureCtx<'_>,
            ) -> Result<(), ProcedureError> {
                $body
            }
        }
    };
}

// WallClock procedure emitting the invocation's `ctx.now()` twice, so
// tests observe both one-value-per-invocation and injected wall values.
clock_procedure!(EmitNowProcedure, "emit_now_procedure", () => u64, clock WallClock,
    |_input, state, ctx| {
        state.apply(1);
        ctx.emit::<u64>(ctx.now())?;
        ctx.emit::<u64>(ctx.now())?;
        Ok(())
    }
);

#[test]
fn injected_wall_clock_serves_one_value_per_invocation() {
    let mut harness =
        ProcedureTestHarness::<EmitNowProcedure>::new().with_wall_clock_script(vec![100, 200]);
    harness.apply_inputs(vec![(), ()]);
    // Both now() calls inside an invocation observe the same scripted
    // value; the next invocation observes the next value.
    harness.expect_emits::<u64>(&[100, 100, 200, 200]);
}

#[test]
fn injected_fixed_wall_clock_makes_wallclock_replay_verifiable() {
    let harness = ProcedureTestHarness::<EmitNowProcedure>::new().with_wall_clock(1720000000000);
    harness
        .replay_emits_equal(&[(), (), ()])
        .expect("fixed injected wall clock must replay identically");
}

/// Event-time extractor: `ctx.now()` comes from the input's first field.
struct TsField;
impl crate::procedure::clock::TimeExtractor<(u64, String)> for TsField {
    fn extract(input: &(u64, String)) -> u64 {
        input.0
    }
}

clock_procedure!(EventTimeProcedure, "event_time_procedure", (u64, String) => u64,
    clock crate::procedure::clock::EventTimeFrom<TsField>,
    |_input, state, ctx| {
        state.apply(1);
        ctx.emit::<u64>(ctx.now())?;
        Ok(())
    }
);

#[test]
fn event_time_clock_extracts_the_dispatched_inputs_timestamp() {
    let mut harness = ProcedureTestHarness::<EventTimeProcedure>::new();
    harness.apply_inputs(vec![
        (1720000000000, "a".to_string()),
        (1720000000500, "b".to_string()),
    ]);
    harness.expect_emits::<u64>(&[1720000000000, 1720000000500]);
}

#[test]
fn event_time_clock_is_replay_stable() {
    let harness = ProcedureTestHarness::<EventTimeProcedure>::new();
    harness
        .replay_emits_equal(&[
            (1720000000000, "a".to_string()),
            (1720000000500, "b".to_string()),
        ])
        .expect("event time depends only on the input");
}

// LogicalClock procedure: `ctx.now()` is the change seq at invocation
// entry (the harness's stand-in for the durable WAL counter).
clock_procedure!(LogicalProcedure, "logical_procedure", () => u64,
    clock crate::procedure::clock::LogicalClock,
    |_input, state, ctx| {
        ctx.emit::<u64>(ctx.now())?;
        state.apply(1);
        Ok(())
    }
);

#[test]
fn logical_clock_advances_with_applied_changes() {
    let mut harness = ProcedureTestHarness::<LogicalProcedure>::new();
    harness.apply_inputs(vec![(), (), ()]);
    // Each invocation applies one change, so entry-time logical values
    // strictly increase across invocations.
    harness.expect_emits::<u64>(&[0, 1, 2]);
}

// -- Stable identifiers + keyed output identity through the harness --

// Keyed procedure: emits under a natural key with a stable_id payload.
clock_procedure!(KeyedIdProcedure, "keyed_id_procedure", String => String, clock WallClock,
    |input, state, ctx| {
        state.apply(1);
        let id_a = ctx.stable_id();
        let id_b = ctx.stable_id();
        assert_ne!(id_a, id_b, "stable_id must be distinct within an invocation");
        ctx.emit_keyed::<String>(input.clone(), format!("{id_a}/{id_b}"))?;
        Ok(())
    }
);

#[test]
fn keyed_emits_carry_identity_stamps_in_the_harness() {
    let mut harness = ProcedureTestHarness::<KeyedIdProcedure>::new().with_keyed_channel();
    harness.apply_inputs(vec!["BTC/1m".to_string()]);
    let metas = harness.emitted_meta();
    assert_eq!(metas.len(), 1);
    let meta = metas[0].expect("keyed emit records stamped metadata");
    assert_eq!(meta.get("flz.entity").unwrap(), "<harness>");
    assert_eq!(meta.get("flz.seq").unwrap(), "0.0");
    assert_eq!(meta.get("flz.key").unwrap(), "BTC/1m");
}

#[test]
fn stable_ids_and_stamps_replay_identically() {
    let harness = ProcedureTestHarness::<KeyedIdProcedure>::new().with_keyed_channel();
    harness
        .replay_emits_equal(&["BTC/1m".to_string(), "ETH/1m".to_string()])
        .expect("ctx-mediated identifiers and stamps must replay byte-identically");
}

// Deliberately non-deterministic: the emitted value comes from ambient
// process state (a static counter), violating the determinism contract.
static AMBIENT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

clock_procedure!(AmbientCounterProcedure, "ambient_counter_procedure", () => u64, clock WallClock,
    |_input, _state, ctx| {
        ctx.emit::<u64>(AMBIENT.fetch_add(1, std::sync::atomic::Ordering::Relaxed))?;
        Ok(())
    }
);

#[test]
fn ambient_non_determinism_fails_replay_emits_equal() {
    let harness = ProcedureTestHarness::<AmbientCounterProcedure>::new();
    let err = harness
        .replay_emits_equal(&[(), ()])
        .expect_err("ambient state must surface as a replay divergence");
    assert!(err.to_string().contains("diverges"));
}
