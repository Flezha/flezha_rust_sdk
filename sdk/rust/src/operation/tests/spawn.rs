//! Operation-requested spawn (`add-operation-requested-spawn`): the request's
//! result mapping, its isolation from `run`, and the replay contract.

use std::sync::mpsc;

use crate::operation::ctx::{EmitSink, OperationCtx};
use crate::operation::error::OperationError;
use crate::operation::spawn::{self, IfRunning, SpawnError, SpawnHandle, SpawnOutcome};
use crate::shutdown::AckResult;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

const IDENTITY: &[u8] = b"spawn/test/";

/// Minimal sink: `spawn_bounded` touches nothing on the emit path, which is
/// exactly what these tests pin.
struct NullSink;

impl EmitSink for NullSink {
    fn emit_inter_operation(
        &mut self,
        _value: Arc<dyn Any + Send + Sync>,
    ) -> Result<(), OperationError> {
        Ok(())
    }

    fn emit_plugin(
        &mut self,
        _value: Arc<dyn Any + Send + Sync>,
        _meta: Option<HashMap<String, String>>,
        _key: Option<String>,
    ) -> Result<(), OperationError> {
        Ok(())
    }

    fn channel_name(&self) -> &str {
        "out"
    }

    fn is_plugin_bound(&self) -> bool {
        false
    }
}

/// Resolve a handle against a controller reply without a control plane.
fn answer(reply: AckResult) -> Result<SpawnOutcome, SpawnError> {
    let (tx, rx) = mpsc::sync_channel(1);
    tx.send(reply).unwrap();
    SpawnHandle::pending("w1".to_string(), "worker".to_string(), rx).wait()
}

#[test]
fn a_spawn_and_a_delivery_are_distinguishable_outcomes() {
    assert_eq!(answer(Ok("spawned".to_string())), Ok(SpawnOutcome::Spawned));
    assert_eq!(
        answer(Ok("delivered".to_string())),
        Ok(SpawnOutcome::Delivered)
    );
}

#[test]
fn the_error_policy_and_an_unknown_factory_come_back_as_errors() {
    assert_eq!(
        answer(Ok("already_running".to_string())),
        Err(SpawnError::AlreadyRunning("w1".to_string()))
    );
    assert_eq!(
        answer(Ok("factory_unknown".to_string())),
        Err(SpawnError::FactoryUnknown("worker".to_string()))
    );
}

#[test]
fn a_refusal_carries_the_controllers_reason() {
    assert_eq!(
        answer(Err("quota_exceeded".to_string())),
        Err(SpawnError::Refused("quota_exceeded".to_string()))
    );
}

#[test]
fn a_reply_that_never_arrives_times_out_rather_than_hanging_forever() {
    // The sender is dropped, so the wait ends on disconnect rather than
    // burning the full deadline.
    let (tx, rx) = mpsc::sync_channel::<AckResult>(1);
    drop(tx);
    assert_eq!(
        SpawnHandle::pending("w1".to_string(), "worker".to_string(), rx).wait(),
        Err(SpawnError::Timeout)
    );
}

#[test]
fn try_result_leaves_a_pending_handle_pending() {
    let (tx, rx) = mpsc::sync_channel(1);
    let mut handle = SpawnHandle::pending("w1".to_string(), "worker".to_string(), rx);
    assert!(handle.try_result().is_none());

    tx.send(Ok("spawned".to_string())).unwrap();
    assert_eq!(handle.try_result(), Some(Ok(SpawnOutcome::Spawned)));
}

#[test]
fn without_a_control_plane_the_request_resolves_instead_of_blocking_the_operation() {
    let _guard = crate::runner::env_test_guard();
    // A trigger whose coordinator is gone — the state a closed runner leaves
    // behind. The request cannot go out, and must not wait out its deadline
    // inside `run` for a reply that can never come.
    let (tx, rx) = crossbeam::channel::unbounded::<crate::runner::RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    drop(rx);

    let handle = spawn::request(
        "w1".to_string(),
        "worker".to_string(),
        b"payload".to_vec(),
        IfRunning::Deliver,
    );
    assert_eq!(handle.wait(), Err(SpawnError::Unavailable));
}

#[test]
fn a_dropped_handle_is_not_an_error_for_the_operation() {
    let _guard = crate::runner::env_test_guard();
    // Fire-and-forget: dropping the handle must neither panic nor block. The
    // failure it carries is reported by the reaper, not by the caller.
    let mut sink = NullSink;
    let ctx = OperationCtx::new(&mut sink, 0, 0, IDENTITY);
    let _ = ctx.spawn_bounded("w1", "worker", b"payload".to_vec(), IfRunning::Deliver);
}

#[test]
fn a_replayed_invocation_issues_no_request() {
    let _guard = crate::runner::env_test_guard();
    let (tx, rx) = crossbeam::channel::unbounded::<crate::runner::RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);

    let mut sink = NullSink;
    let ctx = OperationCtx::new(&mut sink, 0, 0, IDENTITY).during_replay(true);
    let handle = ctx.spawn_bounded("w1", "worker", b"payload".to_vec(), IfRunning::Deliver);

    assert_eq!(handle.wait(), Ok(SpawnOutcome::SkippedOnReplay));
    assert!(rx.try_recv().is_err(), "a replay must reach no coordinator");
}

#[test]
fn a_live_invocation_ships_the_request_the_controller_reads() {
    let _guard = crate::runner::env_test_guard();
    let (tx, rx) = crossbeam::channel::unbounded::<crate::runner::RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);

    let mut sink = NullSink;
    let ctx = OperationCtx::new(&mut sink, 0, 0, IDENTITY);
    let handle = ctx.spawn_bounded("w1", "worker", b"payload".to_vec(), IfRunning::Error);

    match rx.try_recv().expect("a live invocation ships one request") {
        crate::runner::RunnerMessage::EmitControlRequest {
            reference_id,
            command: crate::pb::command_request::Command::SpawnBoundedRequest(request),
        } => {
            assert!(reference_id.starts_with("spawn-bounded-w1-"));
            assert_eq!(request.instance_id, "w1");
            assert_eq!(request.factory_id, "worker");
            assert_eq!(request.payload, b"payload".to_vec());
            assert_eq!(request.if_running, "error");
        }
        _ => panic!("expected a SpawnBoundedRequest on the coordinator channel"),
    }

    // The reply never arrives (no controller behind the channel), so the
    // handle is still pending — dropping it hands it to the reaper.
    drop(handle);
}

#[test]
fn the_policy_is_carried_on_the_wire_as_the_controller_reads_it() {
    assert_eq!(IfRunning::default(), IfRunning::Deliver);
    assert_eq!(IfRunning::Deliver.as_wire(), "deliver");
    assert_eq!(IfRunning::Error.as_wire(), "error");
}
