//! The registration surface and its refusals: absolute times, idempotent
//! re-registration, cancellation, and what a declaration may not ask for.

use flezha_runner::entity::timer::TimerKind;
use flezha_runner::testing::ProcedureTestHarness;

mod common;
use common::*;

// -------------------------------------------------- surface and refusals

/// Spec: "A timer is registered and returns its absolute time" + "Duplicate
/// registration is idempotent" + "Cancellation removes a pending timer".
#[test]
fn the_registration_surface_is_absolute_idempotent_and_cancellable() {
    let mut harness = ProcedureTestHarness::<SurfaceProc>::new().with_wall_clock(1_000);
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

/// Spec: "Registration past the cap is refused" — naming the Procedure and the
/// bound, leaving the pending set unchanged.
#[test]
fn registration_past_the_cap_is_refused_naming_the_procedure_and_the_bound() {
    let err = refusal_of(SurfaceCmd::RegisterMany(4));
    assert!(
        err.contains("timers_surface_procedure") && err.contains('3'),
        "the refusal names the procedure and the bound: {err}"
    );
}

/// Spec: "An event-time timer without an event-time clock is refused" —
/// naming the Procedure and its declared clock, registering nothing.
#[test]
fn an_event_time_timer_under_a_wall_clock_is_refused_naming_the_clock() {
    let err = refusal_of(SurfaceCmd::RegisterEventTime);
    assert!(
        err.contains("timers_surface_procedure") && err.contains("WallClock"),
        "the refusal names the procedure and its clock: {err}"
    );
}

/// Drive one input expecting `run` to fail; the harness panics on `Err`, which
/// is exactly the observation a refusal case wants.
fn refusal_of(input: SurfaceCmd) -> String {
    let payload = std::panic::catch_unwind(move || {
        ProcedureTestHarness::<SurfaceProc>::new()
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
