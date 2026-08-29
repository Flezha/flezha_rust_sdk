//! Run-sugar classes: a `run` body and nothing else.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::workflow::*;

pub(crate) static EMIT_RUNS: AtomicUsize = AtomicUsize::new(0);
pub(crate) static FAIL_RUNS: AtomicUsize = AtomicUsize::new(0);

// ---- run-sugar operatives ------------------------------------------------

/// Run-sugar that echoes its args as plugin output (counts runs).
pub(crate) struct EmitWorkflow;
impl WorkflowOperative for EmitWorkflow {
    fn factory_id() -> &'static str {
        "test-emit-workflow"
    }
    fn run(ctx: &mut WorkflowCtx, args: &str) -> Result<(), WorkflowError> {
        EMIT_RUNS.fetch_add(1, Ordering::SeqCst);
        ctx.emit(args.as_bytes().to_vec())?;
        Ok(())
    }
}

/// Run-sugar that always fails (counting its runs: a `run` body has no
/// entities to scope a retry to, so it must run exactly once).
pub(crate) struct FailWorkflow;
impl WorkflowOperative for FailWorkflow {
    fn factory_id() -> &'static str {
        "test-fail-workflow"
    }
    fn run(_ctx: &mut WorkflowCtx, _args: &str) -> Result<(), WorkflowError> {
        FAIL_RUNS.fetch_add(1, Ordering::SeqCst);
        Err(WorkflowError::from("boom"))
    }
}

pub(crate) struct PanicWorkflow;
impl WorkflowOperative for PanicWorkflow {
    fn factory_id() -> &'static str {
        "test-panic-workflow"
    }
    fn run(_ctx: &mut WorkflowCtx, _args: &str) -> Result<(), WorkflowError> {
        panic!("kaboom")
    }
}

/// Run-sugar that returns a terminal result via `ctx.complete`.
pub(crate) struct CompleteWorkflow;
impl WorkflowOperative for CompleteWorkflow {
    fn factory_id() -> &'static str {
        "test-complete-workflow"
    }
    fn run(ctx: &mut WorkflowCtx, args: &str) -> Result<(), WorkflowError> {
        ctx.complete(format!("result:{args}").into_bytes());
        Ok(())
    }
}

/// Interactive run-sugar: loops on the inbox, echoes `got:<msg>`, exits the
/// loop on a `quit` message (terminal predicate) or on cancellation.
pub(crate) struct LoopWorkflow;
impl WorkflowOperative for LoopWorkflow {
    fn factory_id() -> &'static str {
        "test-loop-workflow"
    }
    fn run(ctx: &mut WorkflowCtx, _args: &str) -> Result<(), WorkflowError> {
        let done = Cell::new(false);
        ctx.loop_until(
            || done.get(),
            |ctx| {
                match ctx.recv() {
                    Some(msg) => {
                        let s = String::from_utf8_lossy(&msg).to_string();
                        if s == "quit" {
                            done.set(true);
                        } else {
                            ctx.emit(format!("got:{s}").into_bytes())?;
                        }
                    }
                    None => done.set(true), // cancellation tripped
                }
                Ok(())
            },
        )
    }
}

/// Interactive run-sugar bounded by `max_duration`: loops on the inbox
/// forever; the only exit is the duration backstop (or external stop).
pub(crate) struct TimedWorkflow;
impl WorkflowOperative for TimedWorkflow {
    fn factory_id() -> &'static str {
        "test-timed-workflow"
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_millis(200))
    }
    fn run(ctx: &mut WorkflowCtx, _args: &str) -> Result<(), WorkflowError> {
        ctx.loop_until(
            || false,
            |ctx| {
                let _ = ctx.recv();
                Ok(())
            },
        )
    }
}

/// External-stop-only: declares it does not self-terminate and has no
/// duration backstop — rejected at registration.
pub(crate) struct ExternalStopOnlyWorkflow;
impl WorkflowOperative for ExternalStopOnlyWorkflow {
    fn factory_id() -> &'static str {
        "test-external-stop-only"
    }
    fn self_terminating() -> bool {
        false
    }
    fn run(_ctx: &mut WorkflowCtx, _args: &str) -> Result<(), WorkflowError> {
        Ok(())
    }
}
