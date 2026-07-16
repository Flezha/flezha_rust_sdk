//! Rust SDK for the Flezha runner platform.
//!
//! # Dispatch model
//!
//! Stream readers push directly into per-Segment lock-free mailboxes
//! (`crossbeam::queue::SegQueue` + `AtomicBool inflight`). A Chase-Lev
//! work-stealing pool drains the mailboxes. A `DashMap`-backed
//! [`operative::registry::OperativeRegistry`] holds live UnboundedOperative
//! instances; the central coordinator runs but only handles
//! control-plane traffic.
//!
//! # Worked example (sticky ordering, with persistent state)
//!
//! ```no_run
//! use flezha_runner::{
//!     Flezha, ChannelBuilder, UnboundedOperative, Operation, OperationCtx, OperationError,
//! };
//! use flezha_runner::state::{
//!     Change, NoChange, RestoreInitPayload, StateHandle, WallClock,
//! };
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, Clone, Default)]
//! struct AppendState { seen: Vec<String> }
//!
//! #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
//! struct AppendChange(String);
//! impl Change for AppendChange {}
//!
//! #[derive(Default)]
//! struct AppendOperation;
//! impl Operation for AppendOperation {
//!     type Input = String;
//!     type Output = String;
//!     type State = AppendState;
//!     type Change = AppendChange;
//!     type Clock = WallClock;
//!     type Config = ();
//!     fn name() -> &'static str { "append_operation" }
//!     fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> AppendState {
//!         AppendState { seen: vec![] }
//!     }
//!     fn apply_change(state: &mut AppendState, change: &AppendChange) {
//!         state.seen.push(change.0.clone());
//!     }
//!     fn run(
//!         &mut self,
//!         input: String,
//!         mut state: StateHandle<'_, Self>,
//!         _: &mut OperationCtx<'_>,
//!     ) -> Result<(), OperationError> {
//!         state.apply(AppendChange(input));
//!         Ok(())
//!     }
//! }
//!
//! struct AppendOperative;
//! impl UnboundedOperative for AppendOperative {
//!     fn factory_id() -> &'static str { "appender" }
//!     fn channels(c: &mut ChannelBuilder) {
//!         c.input::<String>("input").into::<AppendOperation>();
//!     }
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! Flezha::new_runner("my_namespace")?
//!     .define_unbounded::<AppendOperative>()?
//!     .run()?; // .start()? instead to keep the calling thread
//! # Ok(())
//! # }
//! # fn _ref() -> Option<NoChange> { None }
//! ```
//!
//! Method names and lifecycle verbs follow `docs/sdk_naming_conventions.md`
//! (repository root), mirrored 1:1 with the Java SDK.
//!
//! When 10,000 messages targeting one `appender` UnboundedOperative instance arrive
//! on the entry channel, the final `state.seen` vector contains those
//! 10,000 inputs in arrival order — the per-Segment inflight CAS
//! guarantees no two `Operation::run` calls overlap on the same segment.
//!
//! # Reconnect & idempotency
//!
//! The SDK's reconnect loop runs forever with capped exponential
//! backoff. On every successful reconnect the controller re-issues
//! `AddOperative` for every UnboundedOperative instance ProcessHub still has tagged
//! to this runner's profile. The handler is idempotent on duplicate
//! `operative_id`: if the local registry already contains the instance
//! it logs and returns without re-registering, so the existing
//! per-Segment state is preserved across the reconnect.
//!
//! # Tuning
//!
//! - `FLEZHA_WORKER_POOL_SIZE` — number of work-stealing threads
//!   (default: `std::thread::available_parallelism()`).
//! - `FLEZHA_TRANSPORT` — `"uds"` (default) or `"quic"`.
//! - `FLEZHA_QUIC_ADDRESS` — QUIC endpoint address.

pub mod bounded;
pub mod channel;
pub mod command;
pub mod operation;
pub mod operative;
mod runner;
pub mod segment;
mod shutdown;
mod util;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

/// One-stop import path for the `operative-state-persistence-rust` trait
/// surface. Bring everything into scope with
/// `use flezha_runner::state::*;` to write Operations that declare
/// `Change` types and route mutations through `state.apply(...)`.
pub mod state {
    pub use crate::operation::change::{Change, CoalesceKey, NoChange};
    pub use crate::operation::clock::{
        Clock, ClockFor, EventTimeFrom, LogicalClock, TimeExtractor, WallClock,
    };
    pub use crate::operation::state_handle::StateHandle;
    pub use crate::segment::restore::{RestoreError, RestoreInitPayload};
}

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, RwLock, Weak};

pub use bounded::spec::BuildBoundedError;
pub use bounded::{BoundedCtx, BoundedError, BoundedOperative, LoopMode, RollbackMode};
pub use channel::builder::BuildTopologyError;
pub use channel::{Channel, ChannelBuilder, OverflowPolicy, RetryPolicy};
use crossbeam::channel::{Receiver, Sender};
/// Authoring sugar. `#[flezha::operation]`, `#[derive(flezha::State)]`, and
/// `flezha::operative!` expand to the trait surface above; see
/// `docs/sdk_authoring_contract.md`.
pub use flezha_macros::{operation, operative, State};
pub use flz_transport::{service, transport};
/// Re-exported so macro-generated code can `submit!` into the auto-operative
/// collection without the user adding an `inventory` dependency.
pub use inventory;
pub use operation::{
    Change, Clock, ClockFor, CoalesceKey, DerivedState, EventTimeFrom, LogicalClock, NoChange,
    Operation, OperationCtx, OperationError, StableId, StateHandle, TimeExtractor, WallClock,
};
pub use operative::builder::TopologyBuildError;
pub use operative::UnboundedOperative;
pub use segment::{RestoreError, RestoreInitPayload};

/// An operative using the single-operation auto-wire path (`#[flezha::operation(sink = …)]`) or
/// `flezha::operative!` registers without an explicit `.define_unbounded::<R>()` call.
/// Generated code `submit!`s one of these; [`register_inventory_operatives`]
/// harvests them inside [`RunnerBuilder::start`].
pub struct AutoOperative {
    /// The class's controller-facing factory id (for duplicate detection).
    pub factory_id: fn() -> &'static str,
    /// Registers the class topology (`__register_unbounded::<R>()`).
    pub register: fn(),
}

impl AutoOperative {
    pub const fn new(factory_id: fn() -> &'static str, register: fn()) -> Self {
        Self {
            factory_id,
            register,
        }
    }
}

inventory::collect!(AutoOperative);

/// Harvest every auto-registered operative (single-operation auto-wire +
/// `flezha::operative!`) into the class registry. Runs at most once per
/// process — [`RunnerBuilder::start`] calls it on every start, and a runner
/// restarted after a prior one closed must not re-collide with its own first
/// harvest. Panics on a duplicate `factory_id` — across two auto-registered
/// operatives or against a operative already declared via
/// [`RunnerBuilder::define_unbounded`].
pub fn register_inventory_operatives() {
    static HARVESTED: std::sync::Once = std::sync::Once::new();
    HARVESTED.call_once(|| {
        for auto in inventory::iter::<AutoOperative> {
            let factory_id = (auto.factory_id)();
            if operative::registry::get_class(factory_id).is_some() {
                panic!(
                    "flezha: duplicate factory_id `{factory_id}`: an auto-registered operative collides \
                     with another auto-registered or manually-registered operative"
                );
            }
            (auto.register)();
        }
    });
}

use crate::operative::instance::UnboundedInstance;
use crate::runner::{ClientResponseMessage, RunnerMessage};
use crate::shutdown::ShutdownBridge;

pub mod pb {
    #[cfg(not(rust_analyzer))]
    include!("generated/runner_commands.rs");
    #[cfg(rust_analyzer)]
    include!("generated/runner_commands.rs");
}

/// Register an UnboundedOperative class topology in the global class registry.
/// The internal body behind [`RunnerBuilder::define_unbounded`]; kept separate
/// so macro-generated auto-wire code can reuse it via [`__register_unbounded`].
pub(crate) fn register_unbounded_class<R: UnboundedOperative>() -> Result<(), TopologyBuildError> {
    let topology = operative::builder::build_topology_for::<R>()?;
    operative::registry::register_class(R::factory_id(), topology);
    Ok(())
}

/// Register a bounded class spec in the global class registry. The internal
/// body behind [`RunnerBuilder::define_bounded`].
pub(crate) fn register_bounded_class<T: BoundedOperative>() -> Result<(), BuildBoundedError> {
    let spec = bounded::spec::build_spec::<T>()?;
    bounded::registry::register_class(T::factory_id(), spec);
    Ok(())
}

/// Macro plumbing for `#[flezha::operation(...)]` / `flezha::operative!`
/// auto-registration. Not part of the public API — declare classes with
/// [`RunnerBuilder::define_unbounded`]. Panics on an invalid topology: the
/// topology is machine-generated, so a failure here is an SDK bug, not an
/// authoring mistake the user could handle.
#[doc(hidden)]
pub fn __register_unbounded<R: UnboundedOperative>() {
    register_unbounded_class::<R>().expect("flezha: auto-wired operative has an invalid topology");
}

/// Startup failure modes reported by [`Flezha::new_runner`],
/// [`RunnerBuilder::start`] and [`RunnerBuilder::run`].
#[derive(Debug, thiserror::Error)]
pub enum RunnerStartError {
    /// The process-wide singleton guard: a runner is already running in this
    /// process. Close (drop) it before starting another.
    #[error(
        "a Flezha runner is already running in this process for namespace `{namespace}`; \
         at most one runner may be started per process"
    )]
    AlreadyRunning { namespace: String },
    /// [`Flezha::new_runner`] was given an empty namespace.
    #[error("namespace must not be empty")]
    InvalidNamespace,
    /// `.start()` was called with no operative type declared and no
    /// auto-wired (`#[flezha::operation]` / `flezha::operative!`) class in
    /// the binary.
    #[error(
        "no operative type declared: call .define_unbounded::<R>() / .define_bounded::<T>() \
         (or auto-wire with #[flezha::operation] / flezha::operative!) before .start()"
    )]
    NoOperativeDeclared,
    /// The connect handshake failed: the runner thread exited before
    /// delivering its connection confirmation.
    #[error("controller connection failed: {0}")]
    ConnectFailed(String),
}

/// Failure modes of [`RunnerBuilder::run`]. `run()` is `start()` +
/// `await_termination()`, and `await_termination()` cannot fail — so this is
/// exactly the start error set.
pub type RunnerError = RunnerStartError;

/// The one runner slot this process has. `namespace` backs the
/// [`RunnerStartError::AlreadyRunning`] message; `shared` backs
/// [`Flezha::current_runner`]. Claimed at the top of [`RunnerBuilder::start`],
/// released when the owning [`FlezhaRunner`] drops (or `start` fails).
struct ActiveRunner {
    namespace: String,
    shared: Weak<RunnerShared>,
}

static ACTIVE_RUNNER: Mutex<Option<ActiveRunner>> = Mutex::new(None);

/// Static-only entry point of the SDK: [`Flezha::new_runner`] to build and
/// start the process's runner, [`Flezha::current_runner`] to reach it from
/// code that does not control startup ordering. Method names and lifecycle
/// verbs follow `docs/sdk_naming_conventions.md`, mirrored 1:1 with the Java
/// SDK's `com.flezha.sdk.Flezha`.
pub struct Flezha;

impl Flezha {
    /// Begin building this process's runner for `namespace`. Declare every
    /// operative class with [`RunnerBuilder::define_unbounded`] /
    /// [`RunnerBuilder::define_bounded`], then [`RunnerBuilder::start`] (keep
    /// the calling thread) or [`RunnerBuilder::run`] (block until shutdown):
    ///
    /// ```no_run
    /// # use flezha_runner::Flezha;
    /// # #[derive(Default)] struct MyOperation;
    /// # struct MyOperative;
    /// # impl flezha_runner::UnboundedOperative for MyOperative {
    /// #     fn factory_id() -> &'static str { "my_operative" }
    /// #     fn channels(_c: &mut flezha_runner::ChannelBuilder) {}
    /// # }
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// Flezha::new_runner("my_app")?.define_unbounded::<MyOperative>()?.run()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new_runner(namespace: impl Into<String>) -> Result<RunnerBuilder, RunnerStartError> {
        let namespace = namespace.into();
        if namespace.is_empty() {
            return Err(RunnerStartError::InvalidNamespace);
        }
        Ok(RunnerBuilder { namespace })
    }

    /// The process's active runner, if one has been started and not yet
    /// dropped — `None` otherwise. Never fails and never starts a runner as
    /// a side effect. The returned handle observes the runner (`id()`,
    /// `operatives()`, `await_termination()`); dropping it does NOT shut the
    /// runner down — only dropping the handle `start()` returned does.
    pub fn current_runner() -> Option<Arc<FlezhaRunner>> {
        let active = ACTIVE_RUNNER.lock().unwrap();
        active
            .as_ref()
            .and_then(|a| a.shared.upgrade())
            .map(|shared| {
                Arc::new(FlezhaRunner {
                    shared,
                    owner: false,
                })
            })
    }
}

/// Builder returned by [`Flezha::new_runner`]. Declares operative types
/// (`define_*`), then starts the runner (`start` / `run`). `start`/`run`
/// consume `self`: a builder cannot be started twice, and the process-wide
/// singleton guard rejects a second runner while one is active.
pub struct RunnerBuilder {
    namespace: String,
}

impl RunnerBuilder {
    /// Declare an UnboundedOperative class the runtime can spawn instances of
    /// later (via the controller's `AddOperative`). Calls `R::channels(...)`
    /// once and freezes the resulting topology in the global class registry.
    /// An invalid topology (duplicate channel names, unwired channels,
    /// missing `segment_for` overrides, type mismatches, ...) is reported
    /// here, at the declaring call site — not a panic.
    pub fn define_unbounded<R: UnboundedOperative>(self) -> Result<Self, TopologyBuildError> {
        register_unbounded_class::<R>()?;
        Ok(self)
    }

    /// Declare a BoundedOperative class. Sibling of [`Self::define_unbounded`];
    /// builds and freezes the class spec (run-sugar body or structured chain)
    /// and reports invalid authoring here, at the declaring call site.
    pub fn define_bounded<T: BoundedOperative>(self) -> Result<Self, BuildBoundedError> {
        register_bounded_class::<T>()?;
        Ok(self)
    }

    /// Connect to the controller and return the process's runner handle.
    /// Blocks only for the connect handshake (retried with exponential
    /// backoff until the controller is reachable), never for the runner's
    /// ongoing lifetime. At most one runner may be started per process —
    /// a second call fails with [`RunnerStartError::AlreadyRunning`] before
    /// connecting or mutating any shared state.
    pub fn start(self) -> Result<FlezhaRunner, RunnerStartError> {
        // Claim the process-wide slot first: exactly one concurrent start()
        // wins, and the loser must not connect or touch shared state.
        {
            let mut active = ACTIVE_RUNNER.lock().unwrap();
            if let Some(running) = active.as_ref() {
                return Err(RunnerStartError::AlreadyRunning {
                    namespace: running.namespace.clone(),
                });
            }
            *active = Some(ActiveRunner {
                namespace: self.namespace.clone(),
                shared: Weak::new(),
            });
        }

        match self.connect() {
            Ok(runner) => {
                ACTIVE_RUNNER
                    .lock()
                    .unwrap()
                    .as_mut()
                    .expect("runner slot claimed above")
                    .shared = Arc::downgrade(&runner.shared);
                Ok(runner)
            }
            Err(e) => {
                *ACTIVE_RUNNER.lock().unwrap() = None;
                Err(e)
            }
        }
    }

    /// Sugar for standalone binaries: [`Self::start`] followed by
    /// [`FlezhaRunner::await_termination`]. Blocks the calling thread until
    /// the runner shuts down — no keep-alive loop needed in `main()`.
    pub fn run(self) -> Result<(), RunnerError> {
        self.start()?.await_termination();
        Ok(())
    }

    fn connect(self) -> Result<FlezhaRunner, RunnerStartError> {
        // Harvest single-operation auto-wire + `operative!` operatives before
        // the runner starts so the class registry is complete by the time
        // AddOperative arrives.
        register_inventory_operatives();

        if operative::registry::class_factory_ids().is_empty()
            && bounded::registry::bounded_factory_ids().is_empty()
        {
            return Err(RunnerStartError::NoOperativeDeclared);
        }

        let (otx, orx): (Sender<RunnerMessage>, Receiver<RunnerMessage>) =
            crossbeam::channel::unbounded();
        let (ctx, irx): (
            Sender<ClientResponseMessage>,
            Receiver<ClientResponseMessage>,
        ) = crossbeam::channel::unbounded();

        let shutdown_bridge = ShutdownBridge::new();
        shutdown::register_bridge(Arc::clone(&shutdown_bridge));

        let runner_thread = runner::start(
            self.namespace.clone(),
            (otx.clone(), orx),
            ctx,
            Arc::clone(&shutdown_bridge),
        );

        await_confirmation(&irx)?;

        Ok(FlezhaRunner {
            shared: Arc::new(RunnerShared {
                namespace: self.namespace,
                runner_tx: Mutex::new(otx),
                client_rx: Mutex::new(irx),
                _runner_thread: Some(runner_thread),
                shutdown_bridge,
                terminated: Mutex::new(false),
                terminated_cv: Condvar::new(),
            }),
            owner: true,
        })
    }
}

/// Map the runner thread's startup confirmation onto the typed start error:
/// a closed channel means the thread died before confirming — a connect
/// failure the caller handles, never a panic.
fn await_confirmation(irx: &Receiver<ClientResponseMessage>) -> Result<(), RunnerStartError> {
    match irx.recv() {
        Ok(ClientResponseMessage::RunnerIdConfirmation) => Ok(()),
        Ok(_) => Err(RunnerStartError::ConnectFailed(
            "unexpected confirmation message from the runner thread".to_string(),
        )),
        Err(_) => Err(RunnerStartError::ConnectFailed(
            "the runner thread exited before confirming the connection".to_string(),
        )),
    }
}

/// State shared between the owning handle `start()` returned and observer
/// handles from [`Flezha::current_runner`].
struct RunnerShared {
    namespace: String,
    runner_tx: Mutex<crossbeam::channel::Sender<RunnerMessage>>,
    client_rx: Mutex<crossbeam::channel::Receiver<ClientResponseMessage>>,
    _runner_thread: Option<std::thread::JoinHandle<()>>,
    shutdown_bridge: Arc<ShutdownBridge>,
    terminated: Mutex<bool>,
    terminated_cv: Condvar,
}

/// Handle to this process's running runner, returned by
/// [`RunnerBuilder::start`]. Dropping the handle `start()` returned runs the
/// existing graceful-shutdown sequence (ship-fresh-state-on-sdk-shutdown) and
/// releases the singleton slot, so a new runner may be started afterwards.
/// Observer handles from [`Flezha::current_runner`] do not trigger shutdown.
pub struct FlezhaRunner {
    shared: Arc<RunnerShared>,
    owner: bool,
}

impl FlezhaRunner {
    /// The namespace this runner was started for. (The controller-assigned
    /// session id lives on the runner thread and rotates across reconnects;
    /// the namespace is the runner's stable identity.)
    pub fn id(&self) -> &str {
        &self.shared.namespace
    }

    /// Snapshot of the live UnboundedOperative-instance registry. Returned shape
    /// mirrors the pre-`operative-mvp-rust` `get_operatives` for backward
    /// compatibility.
    // reason: public API; refactor would break external callers (test apps, observability)
    #[allow(clippy::type_complexity)]
    pub fn operatives(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<Arc<UnboundedInstance>>>>>> {
        self.shared
            .runner_tx
            .lock()
            .unwrap()
            .send(RunnerMessage::ReturnOperatives)
            .expect("Failed to send ReturnOperatives message");

        let response = self
            .shared
            .client_rx
            .lock()
            .unwrap()
            .recv()
            .expect("Failed to receive operatives");

        match response {
            ClientResponseMessage::ReturnOperatives(operatives) => operatives,
            _ => panic!("Unexpected message type received"),
        }
    }

    /// Block the calling thread until the runner shuts down (the owning
    /// handle drops, or SIGTERM-triggered graceful shutdown exits the
    /// process). Replaces every hand-rolled keep-alive loop.
    pub fn await_termination(&self) {
        let mut terminated = self.shared.terminated.lock().unwrap();
        while !*terminated {
            terminated = self.shared.terminated_cv.wait(terminated).unwrap();
        }
    }
}

/// Drop trigger for the graceful-shutdown sequence — owning handle only.
impl Drop for FlezhaRunner {
    fn drop(&mut self) {
        if !self.owner {
            return;
        }
        self.shared.shutdown_bridge.run_graceful_shutdown();
        *ACTIVE_RUNNER.lock().unwrap() = None;
        *self.shared.terminated.lock().unwrap() = true;
        self.shared.terminated_cv.notify_all();
    }
}

#[cfg(test)]
mod entry_point_tests {
    use super::*;

    #[test]
    fn a_dead_runner_thread_is_a_connect_failed_result() {
        let (ctx, irx) = crossbeam::channel::unbounded::<ClientResponseMessage>();
        // The runner thread panicking/exiting before confirming manifests as
        // every sender dropping — recv() fails. That must map to a typed
        // ConnectFailed, not a panic in the caller's thread.
        drop(ctx);
        match await_confirmation(&irx) {
            Err(RunnerStartError::ConnectFailed(reason)) => {
                assert!(reason.contains("exited before confirming"), "got: {reason}");
            }
            other => panic!("expected ConnectFailed, got: {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Internal dispatch surface for benches/tests. NOT a stable API. Only
// available under the `bench-internals` feature, which is enabled by
// this crate's `[[bench]]` targets via `required-features`. Downstream
// consumers never enable this feature, so the surface below is not part
// of the crate's public API for version-compat purposes.
// ---------------------------------------------------------------------------
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub mod __dispatch_internals {
    pub use crate::runner::fast::OperativeDispatchHandle;
    pub use crate::runner::mailbox::Mailbox;
    pub use crate::runner::work_stealing_pool::WorkStealingPool;
}
