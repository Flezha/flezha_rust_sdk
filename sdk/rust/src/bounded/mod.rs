//! The **bounded** compute-unit kind: a stateless, run-once job.
//!
//! A bounded is started with args, runs its [`BoundedOperative::run`] function exactly
//! once on the executor's work pool, may emit output to bound plugins via
//! [`BoundedCtx::emit`], and signals completion to the controller. It has no
//! operations, no segments, no input channel, and no per-segment state —
//! it shares only the runtime plumbing (instance registry, executor bind,
//! plugin emit) with operatives.
//!
//! Authoring is a distinct, lightweight trait (not a degenerate Operation):
//!
//! ```ignore
//! struct Greet;
//! impl flezha_executor::BoundedOperative for Greet {
//!     fn factory_id() -> &'static str { "greet" }
//!     fn run(ctx: &mut flezha_executor::BoundedCtx, args: &str) -> Result<(), flezha_executor::BoundedError> {
//!         ctx.emit(format!("hello {args}").into_bytes())?;
//!         Ok(())
//!     }
//! }
//! flezha_executor::register_bounded::<Greet>();
//! ```

pub mod registry;

use std::fmt;
use std::sync::Arc;

/// A run-once compute unit. The runtime invokes [`run`](BoundedOperative::run) exactly
/// once per started instance, on a work-pool thread, with `args` taken from
/// the `init_values` shipped at start. Capabilities attach to the kind, not
/// here — choosing the bounded kind is sufficient.
pub trait BoundedOperative: Send + Sync + 'static {
    /// Stable, controller-facing factory identifier.
    fn factory_id() -> &'static str;

    /// Run the job once. Emit output via `ctx`; return `Ok(())` on success
    /// or `Err(BoundedError)` on a terminal failure (the controller tears the
    /// bounded down either way — there is no retry).
    fn run(ctx: &mut BoundedCtx, args: &str) -> Result<(), BoundedError>;
}

/// Type-erased entrypoint stored in the class registry. `T::run` coerces to
/// this `fn` pointer because it takes no `self`.
pub type BoundedRunFn = fn(&mut BoundedCtx, &str) -> Result<(), BoundedError>;

/// A terminal bounded failure.
#[derive(Debug, Clone)]
pub struct BoundedError(pub String);

impl fmt::Display for BoundedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for BoundedError {
    fn from(s: String) -> Self {
        BoundedError(s)
    }
}

impl From<&str> for BoundedError {
    fn from(s: &str) -> Self {
        BoundedError(s.to_string())
    }
}

/// Processing context handed to [`BoundedOperative::run`]. Reuses the operative output path:
/// `emit` writes a frame tagged with the bounded's instance id on a secondary
/// stream, which the controller fans out to the plugins bound to this
/// unit (e.g. Kafka, stdout) — identical to operative channel→plugin output.
pub struct BoundedCtx {
    instance_id: Arc<str>,
}

impl BoundedCtx {
    pub(crate) fn new(instance_id: Arc<str>) -> Self {
        Self { instance_id }
    }

    /// Emit one output payload to the unit's bound plugins. Best-effort and
    /// at-least-once, like operative output. Routed through the coordinator so
    /// the stream write stays single-threaded.
    pub fn emit(&mut self, bytes: impl Into<Vec<u8>>) -> Result<(), BoundedError> {
        crate::executor::emit_bounded_output(self.instance_id.to_string(), bytes.into());
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
    use crate::pb::AddOperative;
    use crate::operative::registry::{OperativeRegistry, Operative};

    static EMIT_RUNS: AtomicUsize = AtomicUsize::new(0);

    // Run-once bounded that echoes its args as plugin output.
    struct EmitBounded;
    impl BoundedOperative for EmitBounded {
        fn factory_id() -> &'static str {
            "test-emit-bounded"
        }
        fn run(ctx: &mut BoundedCtx, args: &str) -> Result<(), BoundedError> {
            EMIT_RUNS.fetch_add(1, Ordering::SeqCst);
            ctx.emit(args.as_bytes().to_vec())?;
            Ok(())
        }
    }

    struct FailBounded;
    impl BoundedOperative for FailBounded {
        fn factory_id() -> &'static str {
            "test-fail-bounded"
        }
        fn run(_ctx: &mut BoundedCtx, _args: &str) -> Result<(), BoundedError> {
            Err(BoundedError::from("boom"))
        }
    }

    struct PanicBounded;
    impl BoundedOperative for PanicBounded {
        fn factory_id() -> &'static str {
            "test-panic-bounded"
        }
        fn run(_ctx: &mut BoundedCtx, _args: &str) -> Result<(), BoundedError> {
            panic!("kaboom")
        }
    }

    #[test]
    fn bounded_error_conversions() {
        let from_str: BoundedError = "x".into();
        assert_eq!(from_str.to_string(), "x");
        let from_string: BoundedError = String::from("y").into();
        assert_eq!(from_string.0, "y");
    }

    #[test]
    fn register_bounded_makes_factory_resolvable() {
        struct RegBounded;
        impl BoundedOperative for RegBounded {
            fn factory_id() -> &'static str {
                "test-getclass-bounded"
            }
            fn run(_c: &mut BoundedCtx, _a: &str) -> Result<(), BoundedError> {
                Ok(())
            }
        }
        assert!(registry::get_class("test-getclass-bounded").is_none());
        crate::register_bounded::<RegBounded>();
        assert!(registry::get_class("test-getclass-bounded").is_some());
        assert!(registry::get_class("test-nonexistent-bounded").is_none());
    }

    #[test]
    fn registry_holds_bounded_as_sealed_variant() {
        let reg = OperativeRegistry::new();
        reg.register_bounded(Arc::from("t1"));

        assert!(reg.contains("t1"));
        // A bounded is not a operative: the operative-only accessor returns None.
        assert!(reg.get_continuous("t1").is_none());
        match reg.get_operative("t1").as_deref() {
            Some(Operative::Bounded(id)) => assert_eq!(&**id, "t1"),
            _ => panic!("expected a BoundedOperative variant"),
        }

        assert!(matches!(reg.remove("t1").as_deref(), Some(Operative::Bounded(_))));
        assert!(!reg.contains("t1"));
    }

    // Owns the process-global coordinator trigger (no other lib test installs
    // it) so it can observe the EmitBoundedOutput / EmitBoundedCompleted messages
    // the run-once driver routes through the coordinator.
    #[test]
    fn start_bounded_runs_once_emits_and_completes() {
        let (tx, rx) = crossbeam::channel::unbounded::<ExecutorMessage>();
        install_reconnect_trigger(tx);

        crate::register_bounded::<EmitBounded>();
        crate::register_bounded::<FailBounded>();
        crate::register_bounded::<PanicBounded>();

        let mut node = ExecutorNode::new("test-ns".to_string());

        // --- success path: run once, args echoed as output, status "ok" ---
        EMIT_RUNS.store(0, Ordering::SeqCst);
        assert!(node.start_bounded("test-emit-bounded".into(), "bounded-ok".into(), "hello".into()));

        let (out, completed) = drain_until_completed(&rx, "bounded-ok");
        assert_eq!(EMIT_RUNS.load(Ordering::SeqCst), 1, "run must execute exactly once");
        assert_eq!(out.as_deref(), Some(b"hello".as_slice()), "args echoed as output");
        assert_eq!(completed, ("ok".to_string(), String::new()));

        // The instance stays registered (controller deregisters on RemOperative);
        // a duplicate AddOperative is idempotent and does NOT re-run.
        assert!(node.has_operative("bounded-ok"));
        assert!(node.get_continuous("bounded-ok").is_none(), "a bounded is not a operative");
        assert!(node.start_bounded("test-emit-bounded".into(), "bounded-ok".into(), "hello".into()));
        assert_eq!(EMIT_RUNS.load(Ordering::SeqCst), 1, "duplicate start must not re-run");

        // --- error path: terminal failure reported via status "error" ---
        assert!(node.start_bounded("test-fail-bounded".into(), "bounded-err".into(), "".into()));
        let (_out, completed_err) = drain_until_completed(&rx, "bounded-err");
        assert_eq!(completed_err.0, "error");
        assert_eq!(completed_err.1, "boom");

        // --- panic path: caught, reported as "error" (worker survives) ---
        assert!(node.start_bounded("test-panic-bounded".into(), "bounded-panic".into(), "".into()));
        let (_out, completed_panic) = drain_until_completed(&rx, "bounded-panic");
        assert_eq!(completed_panic.0, "error");
        assert!(completed_panic.1.contains("kaboom"));

        // --- unknown factory: no instance created ---
        assert!(!node.start_bounded("test-no-such".into(), "bounded-x".into(), "".into()));
        assert!(!node.has_operative("bounded-x"));

        // --- AddOperative --kind bounded routes to the bounded path ---
        let add = AddOperative {
            operative_id: "bounded-via-add".into(),
            factory_id: "test-emit-bounded".into(),
            init_values: "argz".into(),
            kind: "bounded".into(),
        };
        let mut errors = Vec::new();
        add.handle(&mut node, "", &mut errors);
        assert!(errors.is_empty(), "AddOperative --kind bounded should not error: {errors:?}");
        let (out2, completed2) = drain_until_completed(&rx, "bounded-via-add");
        assert_eq!(out2.as_deref(), Some(b"argz".as_slice()));
        assert_eq!(completed2.0, "ok");

        // --- AddOperative --kind bounded for an unregistered factory reports it ---
        let add_bad = AddOperative {
            operative_id: "bounded-bad".into(),
            factory_id: "test-unregistered".into(),
            init_values: "".into(),
            kind: "bounded".into(),
        };
        let mut errors_bad = Vec::new();
        add_bad.handle(&mut node, "", &mut errors_bad);
        assert_eq!(errors_bad.len(), 1);
        assert!(errors_bad[0].contains("BoundedOperative class"));
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
                .unwrap_or_else(|_| panic!("timed out waiting for bounded {id} completion"));
            match msg {
                ExecutorMessage::EmitBoundedOutput { operative_id, payload } if operative_id == id => {
                    out = Some(payload)
                }
                ExecutorMessage::EmitBoundedCompleted {
                    operative_id,
                    status,
                    error,
                } if operative_id == id => return (out, (status, error)),
                _ => {}
            }
        }
    }
}
