//! The **task** compute-unit kind: a stateless, run-once job.
//!
//! A task is started with args, runs its [`Task::run`] function exactly
//! once on the executor's work pool, may emit output to bound plugins via
//! [`TaskCtx::emit`], and signals completion to the controller. It has no
//! shards, no partitions, no input channel, and no per-partition state —
//! it shares only the runtime plumbing (instance registry, executor bind,
//! plugin emit) with relays.
//!
//! Authoring is a distinct, lightweight trait (not a degenerate Shard):
//!
//! ```ignore
//! struct Greet;
//! impl flezha_executor::Task for Greet {
//!     fn factory_id() -> &'static str { "greet" }
//!     fn run(ctx: &mut flezha_executor::TaskCtx, args: &str) -> Result<(), flezha_executor::TaskError> {
//!         ctx.emit(format!("hello {args}").into_bytes())?;
//!         Ok(())
//!     }
//! }
//! flezha_executor::register_task::<Greet>();
//! ```

pub mod registry;

use std::fmt;
use std::sync::Arc;

/// A run-once compute unit. The runtime invokes [`run`](Task::run) exactly
/// once per started instance, on a work-pool thread, with `args` taken from
/// the `init_values` shipped at start. Capabilities attach to the kind, not
/// here — choosing the task kind is sufficient.
pub trait Task: Send + Sync + 'static {
    /// Stable, controller-facing factory identifier.
    fn factory_id() -> &'static str;

    /// Run the job once. Emit output via `ctx`; return `Ok(())` on success
    /// or `Err(TaskError)` on a terminal failure (the controller tears the
    /// task down either way — there is no retry).
    fn run(ctx: &mut TaskCtx, args: &str) -> Result<(), TaskError>;
}

/// Type-erased entrypoint stored in the class registry. `T::run` coerces to
/// this `fn` pointer because it takes no `self`.
pub type TaskRunFn = fn(&mut TaskCtx, &str) -> Result<(), TaskError>;

/// A terminal task failure.
#[derive(Debug, Clone)]
pub struct TaskError(pub String);

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for TaskError {
    fn from(s: String) -> Self {
        TaskError(s)
    }
}

impl From<&str> for TaskError {
    fn from(s: &str) -> Self {
        TaskError(s.to_string())
    }
}

/// Processing context handed to [`Task::run`]. Reuses the relay output path:
/// `emit` writes a frame tagged with the task's instance id on a secondary
/// stream, which the controller fans out to the plugins bound to this
/// unit (e.g. Kafka, stdout) — identical to relay channel→plugin output.
pub struct TaskCtx {
    instance_id: Arc<str>,
}

impl TaskCtx {
    pub(crate) fn new(instance_id: Arc<str>) -> Self {
        Self { instance_id }
    }

    /// Emit one output payload to the unit's bound plugins. Best-effort and
    /// at-least-once, like relay output. Routed through the coordinator so
    /// the stream write stays single-threaded.
    pub fn emit(&mut self, bytes: impl Into<Vec<u8>>) -> Result<(), TaskError> {
        crate::executor::emit_task_output(self.instance_id.to_string(), bytes.into());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use crossbeam::channel::Receiver;

    use crate::command::CommandAction;
    use crate::executor::fast::ExecutorNode;
    use crate::executor::{install_reconnect_trigger, ExecutorMessage};
    use crate::pb::AddRelay;
    use crate::relay::registry::{RelayInstanceRegistry, UnitInstance};

    static EMIT_RUNS: AtomicUsize = AtomicUsize::new(0);

    // Run-once task that echoes its args as plugin output.
    struct EmitTask;
    impl Task for EmitTask {
        fn factory_id() -> &'static str {
            "test-emit-task"
        }
        fn run(ctx: &mut TaskCtx, args: &str) -> Result<(), TaskError> {
            EMIT_RUNS.fetch_add(1, Ordering::SeqCst);
            ctx.emit(args.as_bytes().to_vec())?;
            Ok(())
        }
    }

    struct FailTask;
    impl Task for FailTask {
        fn factory_id() -> &'static str {
            "test-fail-task"
        }
        fn run(_ctx: &mut TaskCtx, _args: &str) -> Result<(), TaskError> {
            Err(TaskError::from("boom"))
        }
    }

    struct PanicTask;
    impl Task for PanicTask {
        fn factory_id() -> &'static str {
            "test-panic-task"
        }
        fn run(_ctx: &mut TaskCtx, _args: &str) -> Result<(), TaskError> {
            panic!("kaboom")
        }
    }

    #[test]
    fn task_error_conversions() {
        let from_str: TaskError = "x".into();
        assert_eq!(from_str.to_string(), "x");
        let from_string: TaskError = String::from("y").into();
        assert_eq!(from_string.0, "y");
    }

    #[test]
    fn register_task_makes_factory_resolvable() {
        struct RegTask;
        impl Task for RegTask {
            fn factory_id() -> &'static str {
                "test-getclass-task"
            }
            fn run(_c: &mut TaskCtx, _a: &str) -> Result<(), TaskError> {
                Ok(())
            }
        }
        assert!(registry::get_class("test-getclass-task").is_none());
        crate::register_task::<RegTask>();
        assert!(registry::get_class("test-getclass-task").is_some());
        assert!(registry::get_class("test-nonexistent-task").is_none());
    }

    #[test]
    fn registry_holds_task_as_sealed_variant() {
        let reg = RelayInstanceRegistry::new();
        reg.register_task(Arc::from("t1"));

        assert!(reg.contains("t1"));
        // A task is not a relay: the relay-only accessor returns None.
        assert!(reg.get_relay("t1").is_none());
        match reg.get_unit("t1").as_deref() {
            Some(UnitInstance::Task(id)) => assert_eq!(&**id, "t1"),
            _ => panic!("expected a Task variant"),
        }

        assert!(matches!(reg.remove("t1").as_deref(), Some(UnitInstance::Task(_))));
        assert!(!reg.contains("t1"));
    }

    // Owns the process-global coordinator trigger (no other lib test installs
    // it) so it can observe the EmitTaskOutput / EmitTaskCompleted messages
    // the run-once driver routes through the coordinator.
    #[test]
    fn start_task_runs_once_emits_and_completes() {
        let (tx, rx) = crossbeam::channel::unbounded::<ExecutorMessage>();
        install_reconnect_trigger(tx);

        crate::register_task::<EmitTask>();
        crate::register_task::<FailTask>();
        crate::register_task::<PanicTask>();

        let mut node = ExecutorNode::new("test-ns".to_string());

        // --- success path: run once, args echoed as output, status "ok" ---
        EMIT_RUNS.store(0, Ordering::SeqCst);
        assert!(node.start_task("test-emit-task".into(), "task-ok".into(), "hello".into()));

        let (out, completed) = drain_until_completed(&rx, "task-ok");
        assert_eq!(EMIT_RUNS.load(Ordering::SeqCst), 1, "run must execute exactly once");
        assert_eq!(out.as_deref(), Some(b"hello".as_slice()), "args echoed as output");
        assert_eq!(completed, ("ok".to_string(), String::new()));

        // The instance stays registered (controller deregisters on RemRelay);
        // a duplicate AddRelay is idempotent and does NOT re-run.
        assert!(node.has_relay("task-ok"));
        assert!(node.get_relay("task-ok").is_none(), "a task is not a relay");
        assert!(node.start_task("test-emit-task".into(), "task-ok".into(), "hello".into()));
        assert_eq!(EMIT_RUNS.load(Ordering::SeqCst), 1, "duplicate start must not re-run");

        // --- error path: terminal failure reported via status "error" ---
        assert!(node.start_task("test-fail-task".into(), "task-err".into(), "".into()));
        let (_out, completed_err) = drain_until_completed(&rx, "task-err");
        assert_eq!(completed_err.0, "error");
        assert_eq!(completed_err.1, "boom");

        // --- panic path: caught, reported as "error" (worker survives) ---
        assert!(node.start_task("test-panic-task".into(), "task-panic".into(), "".into()));
        let (_out, completed_panic) = drain_until_completed(&rx, "task-panic");
        assert_eq!(completed_panic.0, "error");
        assert!(completed_panic.1.contains("kaboom"));

        // --- unknown factory: no instance created ---
        assert!(!node.start_task("test-no-such".into(), "task-x".into(), "".into()));
        assert!(!node.has_relay("task-x"));

        // --- AddRelay --kind task routes to the task path ---
        let add = AddRelay {
            relay_id: "task-via-add".into(),
            factory_id: "test-emit-task".into(),
            init_values: "argz".into(),
            kind: "task".into(),
        };
        let mut errors = Vec::new();
        add.handle(&mut node, "", &mut errors);
        assert!(errors.is_empty(), "AddRelay --kind task should not error: {errors:?}");
        let (out2, completed2) = drain_until_completed(&rx, "task-via-add");
        assert_eq!(out2.as_deref(), Some(b"argz".as_slice()));
        assert_eq!(completed2.0, "ok");

        // --- AddRelay --kind task for an unregistered factory reports it ---
        let add_bad = AddRelay {
            relay_id: "task-bad".into(),
            factory_id: "test-unregistered".into(),
            init_values: "".into(),
            kind: "task".into(),
        };
        let mut errors_bad = Vec::new();
        add_bad.handle(&mut node, "", &mut errors_bad);
        assert_eq!(errors_bad.len(), 1);
        assert!(errors_bad[0].contains("Task class"));
    }

    fn drain_until_completed(
        rx: &Receiver<ExecutorMessage>,
        id: &str,
    ) -> (Option<Vec<u8>>, (String, String)) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut out = None;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let msg = rx
                .recv_timeout(remaining)
                .unwrap_or_else(|_| panic!("timed out waiting for task {id} completion"));
            match msg {
                ExecutorMessage::EmitTaskOutput { relay_id, payload } if relay_id == id => {
                    out = Some(payload)
                }
                ExecutorMessage::EmitTaskCompleted {
                    relay_id,
                    status,
                    error,
                } if relay_id == id => return (out, (status, error)),
                _ => {}
            }
        }
    }
}
