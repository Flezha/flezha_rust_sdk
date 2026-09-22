//! Tasklet-requested spawn (`tasklet-requested-spawn`) from the tasklet's
//! side: the call shapes, the request they ship, their isolation from `run`,
//! and the replay contract. The handle itself is `flz_core::spawn`'s.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::tasklet::ctx::{EmitSink, TaskletCtx};
use crate::tasklet::error::TaskletError;
use crate::tasklet::{IfRunning, SpawnError, SpawnOptions, SpawnOutcome, SpawnResult};

const IDENTITY: &[u8] = b"spawn/test/";

/// Minimal sink: `spawn_workflow` touches nothing on the emit path, which is
/// exactly what these tests pin.
struct NullSink;

impl EmitSink for NullSink {
    fn emit(
        &mut self,
        _value: Arc<dyn Any + Send + Sync>,
        _meta: Option<HashMap<String, String>>,
        _key: Option<String>,
    ) -> Result<(), TaskletError> {
        Ok(())
    }

    fn channel_name(&self) -> &str {
        "out"
    }
}

/// Route control requests to a channel this test reads.
fn capture_control_requests() -> crossbeam::channel::Receiver<crate::runner::RunnerMessage> {
    let (tx, rx) = crossbeam::channel::unbounded::<crate::runner::RunnerMessage>();
    crate::runner::install_reconnect_trigger(tx);
    rx
}

/// The one `SpawnWorkflowRequest` a call shipped, and its reference id.
fn shipped(
    rx: &crossbeam::channel::Receiver<crate::runner::RunnerMessage>,
) -> (String, crate::pb::SpawnWorkflowRequest) {
    let unexpected = "expected a SpawnWorkflowRequest on the coordinator channel";
    let message = rx.try_recv().expect("a live invocation ships one request");
    let crate::runner::RunnerMessage::EmitControlRequest {
        reference_id,
        command,
    } = message
    else {
        panic!("{unexpected}");
    };
    let crate::pb::command_request::Command::SpawnWorkflowRequest(request) = *command else {
        panic!("{unexpected}");
    };
    (reference_id, request)
}

#[test]
fn without_a_control_plane_the_request_resolves_instead_of_blocking_the_tasklet() {
    let _guard = crate::runner::env_test_guard();
    // A trigger whose coordinator is gone: the state a closed runner leaves
    // behind. The request cannot go out, and must not wait out its deadline
    // inside `run` for a reply that can never come.
    drop(capture_control_requests());

    let mut sink = NullSink;
    let ctx = TaskletCtx::new(&mut sink, 0, 0, IDENTITY);
    assert_eq!(
        ctx.spawn_workflow("worker", b"payload".to_vec()).wait(),
        Err(SpawnError::Unavailable)
    );
}

#[test]
fn a_stream_factory_is_answered_by_name() {
    let _guard = crate::runner::env_test_guard();
    let rx = capture_control_requests();
    let mut sink = NullSink;
    let ctx = TaskletCtx::new(&mut sink, 0, 0, IDENTITY);

    let handle = ctx.spawn_workflow("prices", b"p".to_vec());
    let (reference_id, request) = shipped(&rx);
    assert_eq!(request.factory_id, "prices");
    flz_core::shutdown::signal_result(
        &reference_id,
        Ok(flz_core::spawn::encode_ack("factory_not_workflow", "")),
    );

    assert_eq!(
        handle.wait(),
        Err(SpawnError::FactoryNotWorkflow("prices".to_string())),
        "a stream factory is neither unknown nor refused"
    );
}

#[test]
fn a_dropped_handle_is_not_an_error_for_the_tasklet() {
    let _guard = crate::runner::env_test_guard();
    // Fire-and-forget: dropping the handle must neither panic nor block. The
    // failure it carries is reported to the log, not to the caller.
    let mut sink = NullSink;
    let ctx = TaskletCtx::new(&mut sink, 0, 0, IDENTITY);
    let _ = ctx.spawn_workflow_with("worker", b"payload".to_vec(), SpawnOptions::id("w1"));
}

#[test]
fn a_replayed_invocation_issues_no_request_and_answers_the_id_it_was_given() {
    let _guard = crate::runner::env_test_guard();
    let rx = capture_control_requests();

    let mut sink = NullSink;
    let ctx = TaskletCtx::new(&mut sink, 0, 0, IDENTITY).during_replay(true);
    let named = ctx.spawn_workflow_with("worker", b"payload".to_vec(), SpawnOptions::id("w1"));
    let unnamed = ctx.spawn_workflow("worker", b"payload".to_vec());

    assert_eq!(
        named.wait(),
        Ok(SpawnResult {
            id: "w1".to_string(),
            outcome: SpawnOutcome::SkippedOnReplay,
        })
    );
    assert_eq!(
        unnamed.wait(),
        Ok(SpawnResult {
            id: String::new(),
            outcome: SpawnOutcome::SkippedOnReplay,
        }),
        "a replay without an id answers an empty id"
    );
    assert!(rx.try_recv().is_err(), "a replay must reach no coordinator");
}

#[test]
fn a_spawn_with_options_ships_its_id_and_policy() {
    let _guard = crate::runner::env_test_guard();
    let rx = capture_control_requests();

    let mut sink = NullSink;
    let ctx = TaskletCtx::new(&mut sink, 0, 0, IDENTITY);
    let handle = ctx.spawn_workflow_with(
        "worker",
        b"payload".to_vec(),
        SpawnOptions::id("w1").if_running(IfRunning::Error),
    );

    let (reference_id, request) = shipped(&rx);
    assert!(reference_id.starts_with("spawn-workflow-w1-"));
    assert_eq!(request.instance_id, "w1");
    assert_eq!(request.factory_id, "worker");
    assert_eq!(request.payload, b"payload".to_vec());
    assert_eq!(request.if_running, "error");
    assert_eq!(handle.id(), "w1");

    // No controller answers, so the handle is still pending: dropping it
    // hands it to the reaper.
    drop(handle);
}

#[test]
fn an_id_alone_delivers_and_no_id_asks_for_a_generated_one() {
    let _guard = crate::runner::env_test_guard();
    let rx = capture_control_requests();

    let mut sink = NullSink;
    let ctx = TaskletCtx::new(&mut sink, 0, 0, IDENTITY);
    let named = ctx.spawn_workflow_with("worker", b"p".to_vec(), SpawnOptions::id("w1"));
    let unnamed = ctx.spawn_workflow("worker", b"p".to_vec());

    let (_, request) = shipped(&rx);
    assert_eq!(
        (request.instance_id.as_str(), request.if_running.as_str()),
        ("w1", "deliver"),
        "an id's default policy delivers into a running instance"
    );
    let (_, request) = shipped(&rx);
    assert_eq!(
        request.instance_id, "",
        "a spawn without an id leaves the id to the controller"
    );
    assert_eq!(unnamed.id(), "");

    drop((named, unnamed));
}
