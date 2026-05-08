//! Section 4.4: harness self-test.
//!
//! Defines a trivial `CounterShard` with `Change = u32` whose
//! `apply_change` adds the change to the counter. Asserts that
//! `replay_equals` succeeds for the deterministic shard, then injects a
//! non-deterministic mutation in a sibling Shard and asserts
//! `replay_equals` returns `Err(ReplayMismatch)`.

use serde::{Deserialize, Serialize};

use crate::partition::restore::RestoreInitPayload;
use crate::shard::change::Change;
use crate::shard::clock::WallClock;
use crate::shard::ctx::ShardCtx;
use crate::shard::error::ShardError;
use crate::shard::state_handle::StateHandle;
use crate::shard::Shard;
use crate::testing::ShardTestHarness;

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct CounterState {
    sum: u64,
}

#[derive(Default)]
struct CounterShard;

impl Change for u32 {}

impl Shard for CounterShard {
    type Input = u32;
    type Output = ();
    type State = CounterState;
    type Change = u32;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "counter_shard"
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
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        state.apply(input);
        Ok(())
    }
}

#[test]
fn deterministic_shard_replay_equals_succeeds() {
    let mut harness = ShardTestHarness::<CounterShard>::new();
    harness.apply_inputs(vec![1u32, 2, 3, 4]);
    assert_eq!(harness.state().sum, 10);
    harness
        .replay_equals()
        .expect("deterministic replay must equal");
}

#[test]
fn expect_changes_chains() {
    let mut harness = ShardTestHarness::<CounterShard>::new();
    harness.apply_inputs(vec![5u32, 7]);
    harness
        .expect_changes(&[5, 7])
        .expect_state(&CounterState { sum: 12 });
}

// -- Non-deterministic shard: apply_change reads SystemTime::now() --

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct NoisyState {
    seed: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct NoisyChange;
impl Change for NoisyChange {}

#[derive(Default)]
struct NoisyShard;

impl Shard for NoisyShard {
    type Input = ();
    type Output = ();
    type State = NoisyState;
    type Change = NoisyChange;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "noisy_shard"
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
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        state.apply(NoisyChange);
        Ok(())
    }
}

#[test]
fn non_deterministic_shard_replay_equals_fails() {
    let mut harness = ShardTestHarness::<NoisyShard>::new();
    harness.apply_inputs(vec![(); 4]);
    // Sleep so the replay's `SystemTime::now()` calls observe distinct
    // sub-second nanos values from the original run.
    std::thread::sleep(std::time::Duration::from_millis(2));
    let result = harness.replay_equals();
    assert!(
        result.is_err(),
        "non-deterministic apply_change should fail replay_equals"
    );
}
