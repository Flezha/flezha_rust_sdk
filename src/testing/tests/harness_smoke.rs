//! Section 4.4: harness self-test.
//!
//! Defines a trivial `CounterTasklet` with `Change = u32` whose
//! `apply_to` adds the change to the counter. Asserts that
//! `replay_equals` succeeds for the deterministic tasklet, then injects a
//! non-deterministic mutation in a sibling Tasklet and asserts
//! `replay_equals` returns `Err(ReplayMismatch)`.

use serde::{Deserialize, Serialize};

use crate::tasklet::change::Change;
use crate::tasklet::error::TaskletError;
use crate::tasklet::{Ctx, Tasklet};
use crate::testing::TaskletTestHarness;

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct CounterState {
    sum: u64,
}

#[derive(Default)]
struct CounterTasklet;

impl Change<CounterState> for u32 {
    fn apply_to(&self, state: &mut CounterState) {
        state.sum = state.sum.saturating_add(*self as u64);
    }
}

#[flezha_runner::tasklet(name = "counter_tasklet")]
impl Tasklet for CounterTasklet {
    type Input = u32;
    type State = CounterState;
    type Change = u32;

    fn run(&mut self, input: u32, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(input);
        Ok(())
    }
}

#[test]
fn deterministic_tasklet_replay_equals_succeeds() {
    let mut harness = TaskletTestHarness::<CounterTasklet>::new();
    harness.apply_inputs(vec![1u32, 2, 3, 4]);
    assert_eq!(harness.state().sum, 10);
    harness
        .replay_equals()
        .expect("deterministic replay must equal");
}

#[test]
fn expect_changes_chains() {
    let mut harness = TaskletTestHarness::<CounterTasklet>::new();
    harness.apply_inputs(vec![5u32, 7]);
    harness
        .expect_changes(&[5, 7])
        .expect_state(&CounterState { sum: 12 });
}

// -- Non-deterministic tasklet: apply_to reads SystemTime::now() --

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct NoisyState {
    seed: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct NoisyChange;
impl Change<NoisyState> for NoisyChange {
    fn apply_to(&self, state: &mut NoisyState) {
        // Non-deterministic: reads SystemTime::now(). The first
        // application records a non-zero state; the replay records a
        // different value because clock advances.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(0);
        state.seed = state.seed.wrapping_add(nanos);
    }
}

#[derive(Default)]
struct NoisyTasklet;

#[flezha_runner::tasklet(name = "noisy_tasklet")]
impl Tasklet for NoisyTasklet {
    type Input = ();
    type State = NoisyState;
    type Change = NoisyChange;

    fn run(&mut self, _: (), ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(NoisyChange);
        Ok(())
    }
}

#[test]
fn non_deterministic_tasklet_replay_equals_fails() {
    let mut harness = TaskletTestHarness::<NoisyTasklet>::new();
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
        "non-deterministic apply_to should fail replay_equals"
    );
}

// -- Ctx-mediated clocks through the harness --

/// Tasklet over `CounterState` with `Change = u32` (`apply_to` sums); I/O
/// types, clock keys, hooks and run body vary per test.
macro_rules! clock_tasklet {
    ($task:ident, $input:ty => $output:ty, ($($key:tt)*), { $($hooks:tt)* },
     |$inp:tt, $ctx:tt| $body:block) => {
        #[derive(Default)]
        struct $task;
        #[flezha_runner::tasklet($($key)*)]
        impl Tasklet for $task {
            type Input = $input;
            type Output = $output;
            type State = CounterState;
            type Change = u32;
            $($hooks)*
            fn run(&mut self, $inp: $input, $ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
                $body
            }
        }
    };
}

// Wall-clock tasklet emitting the invocation's `ctx.now()` twice, so
// tests observe both one-value-per-invocation and injected wall values.
clock_tasklet!(EmitNowTasklet, () => u64, (name = "emit_now_tasklet"), {},
    |_input, ctx| {
        ctx.change(1);
        ctx.emit(ctx.now())?;
        ctx.emit(ctx.now())?;
        Ok(())
    }
);

#[test]
fn injected_wall_clock_serves_one_value_per_invocation() {
    let mut harness =
        TaskletTestHarness::<EmitNowTasklet>::new().with_wall_clock_script(vec![100, 200]);
    harness.apply_inputs(vec![(), ()]);
    // Both now() calls inside an invocation observe the same scripted
    // value; the next invocation observes the next value.
    harness.expect_emits(&[100, 100, 200, 200]);
}

#[test]
fn injected_fixed_wall_clock_makes_wallclock_replay_verifiable() {
    let harness = TaskletTestHarness::<EmitNowTasklet>::new().with_wall_clock(1720000000000);
    harness
        .replay_emits_equal(&[(), (), ()])
        .expect("fixed injected wall clock must replay identically");
}

// Event time: `ctx.now()` comes from the input's first field.
clock_tasklet!(EventTimeTasklet, (u64, String) => u64, (name = "event_time_tasklet"),
    {
        fn event_time(input: &(u64, String)) -> u64 {
            input.0
        }
    },
    |_input, ctx| {
        ctx.change(1);
        ctx.emit(ctx.now())?;
        Ok(())
    }
);

#[test]
fn event_time_clock_extracts_the_dispatched_inputs_timestamp() {
    let mut harness = TaskletTestHarness::<EventTimeTasklet>::new();
    harness.apply_inputs(vec![
        (1720000000000, "a".to_string()),
        (1720000000500, "b".to_string()),
    ]);
    harness.expect_emits(&[1720000000000, 1720000000500]);
}

#[test]
fn event_time_clock_is_replay_stable() {
    let harness = TaskletTestHarness::<EventTimeTasklet>::new();
    harness
        .replay_emits_equal(&[
            (1720000000000, "a".to_string()),
            (1720000000500, "b".to_string()),
        ])
        .expect("event time depends only on the input");
}

// Logical-clock tasklet: `ctx.now()` is the change seq at invocation
// entry (the harness's stand-in for the durable WAL counter).
clock_tasklet!(LogicalTasklet, () => u64, (name = "logical_tasklet", logical_clock), {},
    |_input, ctx| {
        ctx.emit(ctx.now())?;
        ctx.change(1);
        Ok(())
    }
);

#[test]
fn logical_clock_advances_with_applied_changes() {
    let mut harness = TaskletTestHarness::<LogicalTasklet>::new();
    harness.apply_inputs(vec![(), (), ()]);
    // Each invocation applies one change, so entry-time logical values
    // strictly increase across invocations.
    harness.expect_emits(&[0, 1, 2]);
}

// -- Stable identifiers + keyed output identity through the harness --

// Keyed tasklet: emits under a natural key with a stable_id payload.
clock_tasklet!(KeyedIdTasklet, String => String, (name = "keyed_id_tasklet"), {},
    |input, ctx| {
        ctx.change(1);
        let id_a = ctx.stable_id();
        let id_b = ctx.stable_id();
        assert_ne!(id_a, id_b, "stable_id must be distinct within an invocation");
        ctx.emit_keyed(input.clone(), format!("{id_a}/{id_b}"))?;
        Ok(())
    }
);

#[test]
fn keyed_emits_carry_identity_stamps_in_the_harness() {
    let mut harness = TaskletTestHarness::<KeyedIdTasklet>::new().with_keyed_channel();
    harness.apply_inputs(vec!["BTC/1m".to_string()]);
    let metas = harness.emitted_meta();
    assert_eq!(metas.len(), 1);
    let meta = metas[0].expect("keyed emit records stamped metadata");
    assert_eq!(meta.get("flz.cell").unwrap(), "<harness>");
    assert_eq!(meta.get("flz.seq").unwrap(), "0.0");
    assert_eq!(meta.get("flz.key").unwrap(), "BTC/1m");
}

#[test]
fn stable_ids_and_stamps_replay_identically() {
    let harness = TaskletTestHarness::<KeyedIdTasklet>::new().with_keyed_channel();
    harness
        .replay_emits_equal(&["BTC/1m".to_string(), "ETH/1m".to_string()])
        .expect("ctx-mediated identifiers and stamps must replay byte-identically");
}

// Deliberately non-deterministic: the emitted value comes from ambient
// process state (a static counter), violating the determinism contract.
static AMBIENT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

clock_tasklet!(AmbientCounterTasklet, () => u64, (name = "ambient_counter_tasklet"), {},
    |_input, ctx| {
        ctx.emit(AMBIENT.fetch_add(1, std::sync::atomic::Ordering::Relaxed))?;
        Ok(())
    }
);

#[test]
fn ambient_non_determinism_fails_replay_emits_equal() {
    let harness = TaskletTestHarness::<AmbientCounterTasklet>::new();
    let err = harness
        .replay_emits_equal(&[(), ()])
        .expect_err("ambient state must surface as a replay divergence");
    assert!(err.to_string().contains("diverges"));
}
