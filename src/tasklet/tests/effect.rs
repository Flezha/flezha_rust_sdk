//! `ctx.effect` (`recorded-effects`) from the tasklet's side: a first run keeps
//! its result through the controller, a kept result is served without running
//! the function, the first result the controller kept wins, an invocation with
//! no input-log offset only runs the function, and the refusals.

use super::spawn::{capture_control_requests, NullSink};
use super::ProbeCell;
use crate::tasklet::error::TaskletError;
use crate::ErrorModel;

/// A Cell identity of an instance of its own, so the process-wide result map
/// is never shared between tests.
fn identity(test: &str) -> Vec<u8> {
    flz_core::flusher::encode_cell_identity(test, "probe", "")
}

/// Answer the one `EffectResult` a call ships, from another thread, as the
/// controller does: `first` names a result kept before it.
fn answer_next(
    rx: crossbeam::channel::Receiver<crate::runner::RunnerMessage>,
    first: Option<&'static [u8]>,
) -> std::thread::JoinHandle<flz_core::pb::EffectResult> {
    std::thread::spawn(move || {
        let message = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the call ships its result");
        let crate::runner::RunnerMessage::EmitControlRequest {
            reference_id,
            command,
        } = message
        else {
            panic!("expected a control request");
        };
        let flz_core::pb::command_request::Command::EffectResult(result) = *command else {
            panic!("expected an EffectResult");
        };
        let ack = flz_core::effect_result::encode_ack(first.is_some(), first.unwrap_or_default());
        flz_core::shutdown::signal_result(&reference_id, Ok(ack));
        result
    })
}

#[test]
fn a_first_run_keeps_its_result_and_a_re_run_is_served_it() {
    let _guard = crate::runner::env_test_guard();
    let cell_id = identity("effect_first_run");
    let mut sink = NullSink;
    let mut cell = ProbeCell::default();

    let answered = answer_next(capture_control_requests(), None);
    let ctx = cell.ctx(&mut sink, 0, 0, &cell_id).taking(Some(4));
    let first: Result<String, TaskletError> = ctx.effect("model", || Ok("answer".to_string()));
    assert_eq!(first.unwrap(), "answer");
    let sent = answered.join().expect("the answer");
    assert_eq!((sent.offset, sent.name.as_str()), (4, "model"));
    assert_eq!(sent.payload, br#""answer""#);
    assert_eq!(sent.relay_id, "effect_first_run");

    // A re-run after a bind: the result arrives shipped, and the call reads it.
    let rebound = identity("effect_first_run_rebound");
    let origin = flz_core::origin::InvocationOrigin {
        cell_identity: &rebound,
        offset: Some(4),
    };
    flz_core::effect_result::landed(vec![flz_core::pb::EffectResult {
        identity: origin.effect_identity(flz_core::origin::Phase::Run, "model", 0),
        offset: 4,
        payload: sent.payload,
        cell_identity: rebound.clone(),
        ..Default::default()
    }]);
    let mut sink = NullSink;
    let mut cell = ProbeCell::default();
    let ctx = cell.ctx(&mut sink, 0, 0, &rebound).taking(Some(4));
    let again: Result<String, TaskletError> =
        ctx.effect("model", || panic!("a kept result runs nothing"));
    assert_eq!(again.unwrap(), "answer");
}

#[test]
fn the_result_the_controller_kept_first_is_returned() {
    let _guard = crate::runner::env_test_guard();
    let cell_id = identity("effect_kept_first");
    let mut sink = NullSink;
    let mut cell = ProbeCell::default();

    let answered = answer_next(capture_control_requests(), Some(br#""first""#));
    let ctx = cell.ctx(&mut sink, 0, 0, &cell_id).taking(Some(9));
    let result: Result<String, TaskletError> = ctx.effect("model", || Ok("second".to_string()));
    answered.join().expect("the answer");
    assert_eq!(result.unwrap(), "first");
}

#[test]
fn without_an_offset_the_function_runs_each_time_and_nothing_is_sent() {
    let _guard = crate::runner::env_test_guard();
    let rx = capture_control_requests();
    let cell_id = identity("effect_unrecorded");
    let runs = std::cell::Cell::new(0);
    for _ in 0..2 {
        let mut sink = NullSink;
        let mut cell = ProbeCell::default();
        let ctx = cell.ctx(&mut sink, 0, 0, &cell_id);
        let result: Result<u32, TaskletError> = ctx.effect("model", || {
            runs.set(runs.get() + 1);
            Ok(runs.get())
        });
        assert_eq!(result.unwrap(), runs.get());
    }
    assert_eq!(runs.get(), 2);
    assert!(rx.try_recv().is_err(), "nothing is sent");
}

#[test]
fn the_refusals_name_their_rule_and_a_failing_function_keeps_nothing() {
    let _guard = crate::runner::env_test_guard();
    let rx = capture_control_requests();
    let cell_id = identity("effect_refusals");
    let mut sink = NullSink;
    let mut cell = ProbeCell::default();
    let ctx = cell.ctx(&mut sink, 0, 0, &cell_id).taking(Some(1));

    let code = |result: Result<u8, TaskletError>| result.unwrap_err().code();
    assert_eq!(code(ctx.effect("", || Ok(1))), "effect.bad_name");
    assert_eq!(
        code(ctx.effect("outer", || ctx.effect("inner", || Ok(1)))),
        "effect.nested"
    );
    let failed = ctx.effect("model", || -> Result<u8, TaskletError> {
        Err(TaskletError::Custom("the model is down".into()))
    });
    assert!(matches!(failed, Err(TaskletError::Custom(ref m)) if m == "the model is down"));
    assert!(
        rx.try_recv().is_err(),
        "a refused or failed call sends nothing"
    );

    let not_recorded = TaskletError::Effect(flz_core::Code::EffectNotRecorded.with([]));
    assert!(
        !not_recorded.is_non_retryable(),
        "an unkept result is retried"
    );
    let too_large = TaskletError::Effect(flz_core::Code::EffectTooLarge.with([]));
    assert!(too_large.is_non_retryable());
}
