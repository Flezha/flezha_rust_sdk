//! The registration surface and its refusals: absolute times, idempotent
//! re-registration, cancellation, and what a declaration may not ask for.

use flezha_runner::cell::timer::TimerKind;
use flezha_runner::testing::TaskletTestHarness;

mod common;
use common::prelude::*;

// -------------------------------------------------- surface and refusals

/// Spec: "A timer is registered and returns its absolute time" + "Duplicate
/// registration is idempotent" + "Cancellation removes a pending timer".
#[test]
fn the_registration_surface_is_absolute_idempotent_and_cancellable() {
    let mut harness = TaskletTestHarness::<SurfaceTask>::new().with_wall_clock(1_000);
    harness.apply_inputs(vec![SurfaceCmd::Register]);
    assert_eq!(
        harness.pending_timers(TimerKind::ProcessingTime),
        vec![31_000],
        "timer_after(30s) at now=1000 is scheduled at 31000"
    );

    harness.apply_inputs(vec![SurfaceCmd::Register]);
    assert_eq!(
        harness.pending_timers(TimerKind::ProcessingTime),
        vec![31_000],
        "pending timers are a set: re-registering the same instant is a no-op"
    );
    assert_eq!(
        harness.recorded_timer_ops().len(),
        1,
        "and the no-op produces no second durable op"
    );

    harness.apply_inputs(vec![SurfaceCmd::Cancel]);
    assert!(harness.pending_timers(TimerKind::ProcessingTime).is_empty());
}

/// `at: 10` arms one timer of each kind at 1000; `at: 20` cancels the
/// event-time one, any later input the processing-time one.
#[derive(Default)]
struct KindCancelTask;
#[flezha_runner::tasklet(name = "timers_kind_cancel_tasklet")]
impl Tasklet for KindCancelTask {
    type Input = Tick;
    fn event_time(input: &Tick) -> u64 {
        input.at
    }
    fn run(&mut self, input: Tick, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        match input.at {
            10 => {
                ctx.timer_at_event_time(1_000)?;
                ctx.timer_after(Duration::from_millis(990))?;
            }
            20 => assert!(ctx.cancel_timer(TimerKind::EventTime, 1_000)?),
            _ => assert!(ctx.cancel_timer(TimerKind::ProcessingTime, 1_000)?),
        }
        Ok(())
    }
}

/// Spec: "Cancellation removes a pending timer" — of the kind named, leaving
/// the other kind's timer at the same instant pending.
#[test]
fn cancelling_an_event_time_timer_leaves_the_processing_time_one() {
    let mut harness = TaskletTestHarness::<KindCancelTask>::new();
    harness.apply_inputs(vec![Tick { at: 10 }, Tick { at: 20 }]);
    assert!(harness.pending_timers(TimerKind::EventTime).is_empty());
    assert_eq!(
        harness.pending_timers(TimerKind::ProcessingTime),
        vec![1_000]
    );
}

#[test]
fn cancelling_a_processing_time_timer_leaves_the_event_time_one() {
    let mut harness = TaskletTestHarness::<KindCancelTask>::new();
    harness.apply_inputs(vec![Tick { at: 10 }, Tick { at: 30 }]);
    assert!(harness.pending_timers(TimerKind::ProcessingTime).is_empty());
    assert_eq!(harness.pending_timers(TimerKind::EventTime), vec![1_000]);
}

/// Spec: "Registration past the cap is refused" — naming the Tasklet and the
/// bound, leaving the pending set unchanged.
#[test]
fn registration_past_the_cap_is_refused_naming_the_tasklet_and_the_bound() {
    let err = refusal_of(SurfaceCmd::RegisterMany(4));
    assert!(
        err.contains("timers_surface_tasklet") && err.contains('3'),
        "the refusal names the tasklet and the bound: {err}"
    );
}

/// Spec: "An event-time timer without an event-time clock is refused" —
/// naming the Tasklet and its declared clock, registering nothing.
#[test]
fn an_event_time_timer_under_a_wall_clock_is_refused_naming_the_clock() {
    let err = refusal_of(SurfaceCmd::RegisterEventTime);
    assert!(
        err.contains("timers_surface_tasklet") && err.contains("'wall'"),
        "the refusal names the tasklet and its clock: {err}"
    );
}

/// Drive one input expecting `run` to fail; the harness panics on `Err`, which
/// is exactly the observation a refusal case wants.
fn refusal_of(input: SurfaceCmd) -> String {
    let payload = std::panic::catch_unwind(move || {
        TaskletTestHarness::<SurfaceTask>::new()
            .with_wall_clock(1_000)
            .apply_inputs(vec![input]);
    })
    .expect_err("the refusal must surface");
    if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else {
        String::from("<non-string panic>")
    }
}
