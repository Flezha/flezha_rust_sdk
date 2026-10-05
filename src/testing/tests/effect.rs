//! The harness's replay pass and `ctx.effect` (`recorded-effects`): the live
//! pass keeps each result, the replay pass is served it, and a replay that
//! runs a function, or never asks for a kept result, fails naming the effect.

use std::sync::atomic::{AtomicU32, Ordering};

use crate::tasklet::error::TaskletError;
use crate::tasklet::{Ctx, NoChange, Tasklet};
use crate::testing::TaskletTestHarness;

/// How many times each tasklet's outside call ran, and how many invocations
/// it has had: one counter pair per tasklet, as the tests run in parallel.
static MODEL_CALLS: AtomicU32 = AtomicU32::new(0);
static DIRECT_INVOCATIONS: AtomicU32 = AtomicU32::new(0);
static EXTRA_INVOCATIONS: AtomicU32 = AtomicU32::new(0);

fn model(calls: &AtomicU32) -> Result<String, TaskletError> {
    Ok(format!("answer-{}", calls.fetch_add(1, Ordering::SeqCst)))
}

/// Emits what its outside call answers, through `ctx.effect`.
#[derive(Default)]
struct Answering;

#[flezha_runner::tasklet(name = "effect_answering")]
impl Tasklet for Answering {
    type Input = u32;
    type Output = String;
    type Change = NoChange;

    fn run(&mut self, _: u32, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let answer = ctx.effect("model", || model(&MODEL_CALLS))?;
        ctx.emit(answer)
    }
}

/// Goes through `ctx.effect` on the live pass and calls the model directly on
/// the replay pass.
#[derive(Default)]
struct AroundTheEffect;

#[flezha_runner::tasklet(name = "effect_around")]
impl Tasklet for AroundTheEffect {
    type Input = u32;
    type Output = String;
    type Change = NoChange;

    fn run(&mut self, _: u32, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        static CALLS: AtomicU32 = AtomicU32::new(0);
        let answer = match DIRECT_INVOCATIONS.fetch_add(1, Ordering::SeqCst) {
            0 => ctx.effect("model", || model(&CALLS))?,
            _ => model(&CALLS)?,
        };
        ctx.emit(answer)
    }
}

/// Makes one more call of the name on the replay pass than on the live one.
#[derive(Default)]
struct OneMoreOnReplay;

#[flezha_runner::tasklet(name = "effect_one_more")]
impl Tasklet for OneMoreOnReplay {
    type Input = u32;
    type Output = String;
    type Change = NoChange;

    fn run(&mut self, _: u32, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let calls = 1 + EXTRA_INVOCATIONS.fetch_add(1, Ordering::SeqCst);
        for _ in 0..calls {
            ctx.effect("model", || Ok::<_, TaskletError>("same".to_string()))?;
        }
        Ok(())
    }
}

#[test]
fn an_outside_call_through_effect_replays_equal() {
    let harness = TaskletTestHarness::<Answering>::new();
    harness
        .replay_emits_equal(&[1, 2])
        .expect("the replay pass is served the live pass's answers");
    assert_eq!(
        MODEL_CALLS.load(Ordering::SeqCst),
        2,
        "the model ran once per input, on the live pass only"
    );
}

#[test]
fn a_call_made_around_the_effect_on_replay_fails_naming_it() {
    let harness = TaskletTestHarness::<AroundTheEffect>::new();
    let mismatch = harness.replay_emits_equal(&[1]).unwrap_err();
    assert!(
        mismatch.detail.contains("effect `model`"),
        "{}",
        mismatch.detail
    );
}

#[test]
fn a_function_that_runs_on_replay_fails_naming_its_effect() {
    let harness = TaskletTestHarness::<OneMoreOnReplay>::new();
    let mismatch = harness.replay_emits_equal(&[1]).unwrap_err();
    assert!(
        mismatch
            .detail
            .contains("effect `model` ran its function on the replay pass"),
        "{}",
        mismatch.detail
    );
}
