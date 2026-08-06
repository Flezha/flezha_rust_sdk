//! Rust SDK for the Flezha runner platform.
//!
//! # Dispatch model
//!
//! Stream readers push directly into per-Entity lock-free mailboxes
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
//!     Flezha, ChannelBuilder, UnboundedOperative, Procedure, ProcedureCtx, ProcedureError,
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
//! struct AppendProcedure;
//! impl Procedure for AppendProcedure {
//!     type Input = String;
//!     type Output = String;
//!     type State = AppendState;
//!     type Change = AppendChange;
//!     type Clock = WallClock;
//!     type Config = ();
//!     fn name() -> &'static str { "append_procedure" }
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
//!         _: &mut ProcedureCtx<'_>,
//!     ) -> Result<(), ProcedureError> {
//!         state.apply(AppendChange(input));
//!         Ok(())
//!     }
//! }
//!
//! struct AppendOperative;
//! impl UnboundedOperative for AppendOperative {
//!     fn factory_id() -> &'static str { "appender" }
//!     fn channels(c: &ChannelBuilder) {
//!         c.input::<String>("input").into::<AppendProcedure>();
//!     }
//! }
//!
//! # fn main() -> Result<(), flezha_runner::FlezhaError> {
//! Flezha::create_runner("my_namespace")?
//!     .define_unbounded::<AppendOperative>()?
//!     .run()?; // `let runner = ...start()?;` instead to keep the calling thread
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
//! 10,000 inputs in arrival order — the per-Entity inflight CAS
//! guarantees no two `Procedure::run` calls overlap on the same entity.
//!
//! # Reconnect & idempotency
//!
//! The SDK's reconnect loop runs forever with capped exponential
//! backoff. On every successful reconnect the controller re-issues
//! `AddOperative` for every UnboundedOperative instance ProcessHub still has tagged
//! to this runner's profile. The handler is idempotent on duplicate
//! `operative_id`: if the local registry already contains the instance
//! it logs and returns without re-registering, so the existing
//! per-Entity state is preserved across the reconnect.
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
pub mod entity;
pub mod operative;
pub mod procedure;
mod runner;
mod shutdown;
mod util;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

/// One-stop import path for the `operative-state-persistence` trait
/// surface. Bring everything into scope with
/// `use flezha_runner::state::*;` to write Procedures that declare
/// `Change` types and route mutations through `state.apply(...)`.
pub mod state {
    pub use crate::entity::restore::{RestoreError, RestoreInitPayload};
    pub use crate::procedure::change::{Change, CoalesceKey, NoChange};
    pub use crate::procedure::clock::{
        Clock, ClockFor, EventTimeFrom, LogicalClock, TimeExtractor, WallClock,
    };
    pub use crate::procedure::state_handle::StateHandle;
}

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, RwLock, Weak};

pub use bounded::spec::BuildBoundedError;
pub use bounded::{BoundedCtx, BoundedError, BoundedOperative, LoopMode, RollbackMode};
pub use channel::builder::TopologyIssue;
pub use channel::{
    Channel, ChannelBuilder, EntryStream, Merge, MergeSource, OverflowPolicy, RetryPolicy, Stream,
};
use crossbeam::channel::{Receiver, Sender};
pub use entity::{RestoreError, RestoreInitPayload};
/// Authoring sugar. `#[flezha::procedure]`, `#[derive(flezha::State)]`, and
/// `flezha::operative!` expand to the trait surface above; see
/// `docs/sdk_authoring_contract.md`.
pub use flezha_macros::{operative, procedure, State};
pub use flz_transport::{service, transport};
/// Re-exported so macro-generated code can `submit!` into the auto-operative
/// collection without the user adding an `inventory` dependency.
pub use inventory;
pub use operative::builder::TopologyBuildError;
pub use operative::UnboundedOperative;
pub use procedure::{
    Change, Clock, ClockFor, CoalesceKey, DerivedState, EventTimeFrom, LogicalClock, NoChange,
    Procedure, ProcedureCtx, ProcedureError, StableId, StateHandle, TimeExtractor, WallClock,
};

/// An operative using the single-procedure auto-wire path (`#[flezha::procedure(sink = …)]`) or
/// `flezha::operative!` registers without an explicit `.define_unbounded::<R>()` call.
/// Generated code `submit!`s one of these; every [`FlezhaRunner::start`]
/// harvests the collection into the runner's declared set.
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

/// Harvest every auto-registered operative (single-procedure auto-wire +
/// `flezha::operative!`) into the class registries WITHOUT a runner —
/// idempotent (the registries overwrite on insert). A started runner does
/// not need this: [`FlezhaRunner::start`] runs the same harvest and merges
/// the ids into its own (empty-at-creation) declared set.
pub fn register_inventory_operatives() {
    for auto in inventory::iter::<AutoOperative> {
        (auto.register)();
    }
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
/// The internal body behind [`FlezhaRunner::define_unbounded`]; kept separate
/// so macro-generated auto-wire code can reuse it via [`__register_unbounded`].
pub(crate) fn register_unbounded_class<R: UnboundedOperative>() -> Result<(), TopologyBuildError> {
    let topology = operative::builder::build_topology_for::<R>()?;
    operative::registry::register_class(R::factory_id(), topology);
    Ok(())
}

/// Register a bounded class spec in the global class registry — the same
/// body `FlezhaRunner::define_bounded` runs, kept callable without a runner
/// for the bounded driver's unit scenarios.
#[cfg(test)]
pub(crate) fn register_bounded_class<T: BoundedOperative>() -> Result<(), BuildBoundedError> {
    let spec = bounded::spec::build_spec::<T>()?;
    bounded::registry::register_class(T::factory_id(), spec);
    Ok(())
}

/// Macro plumbing for `#[flezha::procedure(...)]` / `flezha::operative!`
/// auto-registration. Not part of the public API — declare classes with
/// [`FlezhaRunner::define_unbounded`]. Panics on an invalid topology: the
/// topology is machine-generated, so a failure here is an SDK bug, not an
/// authoring mistake the user could handle.
#[doc(hidden)]
pub fn __register_unbounded<R: UnboundedOperative>() {
    register_unbounded_class::<R>().expect("flezha: auto-wired operative has an invalid topology");
}

/// The ONE public error of the entry-point chain: every fallible call on
/// [`FlezhaRunner`] returns `Result<_, FlezhaError>`, so
/// `Flezha::create_runner(ns)?.define_unbounded::<A>()?.run()?` compiles
/// under a single `?`. Java throws the identically-named failure classes
/// (naming rule 6 in `docs/sdk_naming_conventions.md`).
#[derive(Debug, thiserror::Error)]
pub enum FlezhaError {
    /// [`Flezha::create_runner`] was given an empty namespace.
    #[error("namespace must not be empty")]
    InvalidNamespace,
    /// The process-wide singleton guard: a runner is already started in this
    /// process (or `.start()` was called twice on one runner). Close (drop)
    /// it before starting another.
    #[error(
        "a Flezha runner is already running in this process for namespace `{namespace}`; \
         at most one runner may be started per process"
    )]
    AlreadyRunning { namespace: String },
    /// The connect handshake failed: the runner thread exited before
    /// delivering its connection confirmation.
    #[error("controller connection failed: {0}")]
    ConnectFailed(String),
    /// `define_unbounded` found an invalid topology (duplicate channel names,
    /// unwired channels, type mismatches, ...) — reported at the declaring
    /// call site, never a panic.
    #[error(transparent)]
    InvalidTopology(#[from] TopologyBuildError),
    /// `define_bounded` found invalid bounded authoring.
    #[error(transparent)]
    InvalidBounded(#[from] BuildBoundedError),
    /// The factory id is already in this runner's declared set.
    #[error("factory `{factory_id}` is already defined on this runner")]
    DuplicateFactoryId { factory_id: String },
    /// `remove_*` named a factory that is not in this runner's declared set.
    #[error("factory `{factory_id}` is not defined on this runner")]
    NotDefined { factory_id: String },
    /// `remove_*` refused: live operative instances of the factory exist.
    /// The type remains defined.
    #[error(
        "factory `{factory_id}` has {live_instances} live operative instance(s); \
         stop them before removing the type"
    )]
    OperativeTypeInUse {
        factory_id: String,
        live_instances: usize,
    },
    /// The call requires a started runner (`await_termination`, `operatives`).
    #[error("runner not started")]
    NotStarted,
    /// The runner thread is gone or replied with an unexpected message.
    #[error("runner unavailable: {0}")]
    RunnerUnavailable(String),
}

/// The one runner slot this process has. `namespace` backs the
/// [`FlezhaError::AlreadyRunning`] message; `core` backs
/// [`Flezha::current_runner`]. Claimed by CAS inside [`FlezhaRunner::start`]
/// (never by [`Flezha::create_runner`]), released when the started owning
/// [`FlezhaRunner`] drops (or `start` fails).
struct ActiveRunner {
    namespace: String,
    core: Weak<RunnerCore>,
}

static ACTIVE_RUNNER: Mutex<Option<ActiveRunner>> = Mutex::new(None);

/// Static-only entry point of the SDK: [`Flezha::create_runner`] to create
/// the process's runner, [`Flezha::current_runner`] to reach it from code
/// that does not control startup ordering. Method names and lifecycle verbs
/// follow `docs/sdk_naming_conventions.md`, mirrored 1:1 with the Java SDK's
/// `com.flezha.sdk.Flezha`.
pub struct Flezha;

impl Flezha {
    /// Create this process's runner for `namespace` — one object carries
    /// declaration and lifecycle (Beam's `Pipeline.create()` shape). Declare
    /// operative classes with [`FlezhaRunner::define_unbounded`] /
    /// [`FlezhaRunner::define_bounded`] (legal before AND after start), then
    /// [`FlezhaRunner::start`] (keep the calling thread) or
    /// [`FlezhaRunner::run`] (block until shutdown). Creating a runner never
    /// connects, claims the process singleton slot, or spawns a thread.
    ///
    /// ```no_run
    /// # use flezha_runner::{Flezha, FlezhaError};
    /// # struct MyOperative;
    /// # impl flezha_runner::UnboundedOperative for MyOperative {
    /// #     fn factory_id() -> &'static str { "my_operative" }
    /// #     fn channels(_c: &flezha_runner::ChannelBuilder) {}
    /// # }
    /// # fn main() -> Result<(), FlezhaError> {
    /// Flezha::create_runner("my_app")?.define_unbounded::<MyOperative>()?.run()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_runner(namespace: impl Into<String>) -> Result<FlezhaRunner, FlezhaError> {
        let namespace = namespace.into();
        if namespace.is_empty() {
            return Err(FlezhaError::InvalidNamespace);
        }
        Ok(FlezhaRunner {
            core: Arc::new(RunnerCore {
                namespace,
                declared: Arc::new(RwLock::new(Vec::new())),
                started: Mutex::new(None),
                terminated: Mutex::new(false),
                terminated_cv: Condvar::new(),
            }),
            owner: true,
        })
    }

    /// The process's active runner, if one has been started and not yet
    /// dropped — `None` otherwise. Never fails and never starts a runner as
    /// a side effect. Runners that were created but never started never
    /// appear here. The returned handle observes the runner (`namespace()`,
    /// `operatives()`, `await_termination()`); dropping it does NOT shut the
    /// runner down — only dropping the started owning handle does.
    pub fn current_runner() -> Option<Arc<FlezhaRunner>> {
        let active = ACTIVE_RUNNER.lock().unwrap();
        active
            .as_ref()
            .and_then(|a| a.core.upgrade())
            .map(|core| Arc::new(FlezhaRunner { core, owner: false }))
    }
}

/// Map the runner thread's startup confirmation onto the typed start error:
/// a closed channel means the thread died before confirming — a connect
/// failure the caller handles, never a panic.
fn await_confirmation(irx: &Receiver<ClientResponseMessage>) -> Result<(), FlezhaError> {
    match irx.recv() {
        Ok(ClientResponseMessage::RunnerIdConfirmation) => Ok(()),
        Ok(_) => Err(FlezhaError::ConnectFailed(
            "unexpected confirmation message from the runner thread".to_string(),
        )),
        Err(_) => Err(FlezhaError::ConnectFailed(
            "the runner thread exited before confirming the connection".to_string(),
        )),
    }
}

/// State shared between the owning handle [`Flezha::create_runner`] returned
/// and observer handles from [`Flezha::current_runner`]. `declared` is this
/// runner's factory set — per-runner, empty at creation (generation 2 must
/// re-define its types); the runner thread serializes it into every
/// connect/reconnect init frame, so post-start mutations resync naturally.
struct RunnerCore {
    namespace: String,
    declared: Arc<RwLock<Vec<String>>>,
    started: Mutex<Option<Started>>,
    terminated: Mutex<bool>,
    terminated_cv: Condvar,
}

/// The runner's live-thread plumbing, present only after a successful
/// `start()`. `client_rx` is locked across each send+recv exchange so two
/// concurrent request/response round-trips never interleave replies.
struct Started {
    runner_tx: Sender<RunnerMessage>,
    client_rx: Mutex<Receiver<ClientResponseMessage>>,
    _runner_thread: std::thread::JoinHandle<()>,
    shutdown_bridge: Arc<ShutdownBridge>,
}

impl Started {
    /// One serialized request/reply round-trip with the runner thread: the
    /// `client_rx` lock is held across send+recv, so the reply belongs to
    /// this request by construction.
    fn exchange(&self, request: RunnerMessage) -> Result<ClientResponseMessage, FlezhaError> {
        let unavailable = || FlezhaError::RunnerUnavailable("runner thread exited".to_string());
        let rx = self.client_rx.lock().unwrap();
        self.runner_tx.send(request).map_err(|_| unavailable())?;
        rx.recv().map_err(|_| unavailable())
    }
}

/// This process's runner: create → mutate (`define_*` / `remove_*`) → start
/// on one object, returned by [`Flezha::create_runner`]. Dropping a started
/// owning handle runs the graceful-shutdown sequence
/// (ship-fresh-state-on-sdk-shutdown) and releases the singleton slot, so a
/// new runner may be started afterwards; dropping a never-started one is a
/// no-op. Observer handles from [`Flezha::current_runner`] never trigger
/// shutdown.
pub struct FlezhaRunner {
    core: Arc<RunnerCore>,
    owner: bool,
}

impl FlezhaRunner {
    /// The namespace this runner was created for. (The controller-assigned
    /// session id lives on the runner thread and rotates across reconnects;
    /// the namespace is the runner's stable identity.)
    pub fn namespace(&self) -> &str {
        &self.core.namespace
    }

    /// Declare an UnboundedOperative class the runtime can spawn instances of
    /// later (via the controller's `AddOperative`). Calls `R::channels(...)`
    /// once and records the topology in the global class registry. Legal
    /// before and after `start()` — a post-start define ships the runner's
    /// full factory set to the controller (`UpdateFactories`). An invalid
    /// topology (duplicate channel names, unwired channels, missing
    /// `entities_for` overrides, type mismatches, ...) is reported here, at
    /// the declaring call site — not a panic.
    pub fn define_unbounded<R: UnboundedOperative>(&self) -> Result<&Self, FlezhaError> {
        // Build/validate BEFORE touching the declared set: an invalid
        // topology must leave the runner exactly as it was.
        let topology = operative::builder::build_topology_for::<R>()?;
        self.declare(R::factory_id(), || {
            operative::registry::register_class(R::factory_id(), topology)
        })?;
        Ok(self)
    }

    /// Declare a BoundedOperative class. Sibling of [`Self::define_unbounded`];
    /// builds the class spec (run-sugar body or structured chain) and reports
    /// invalid authoring here, at the declaring call site.
    pub fn define_bounded<T: BoundedOperative>(&self) -> Result<&Self, FlezhaError> {
        let spec = bounded::spec::build_spec::<T>()?;
        self.declare(T::factory_id(), || {
            bounded::registry::register_class(T::factory_id(), spec)
        })?;
        Ok(self)
    }

    /// Remove an UnboundedOperative class from this runner. Legal before and
    /// after `start()`. Refuses with
    /// [`FlezhaError::OperativeTypeInUse`] while live instances of the
    /// factory exist (the type stays defined) and with
    /// [`FlezhaError::NotDefined`] for a factory this runner never declared.
    /// A post-start remove ships the remaining full factory set to the
    /// controller (`UpdateFactories`).
    pub fn remove_unbounded<R: UnboundedOperative>(&self) -> Result<&Self, FlezhaError> {
        self.remove(R::factory_id(), false)
    }

    /// Remove a BoundedOperative class. Sibling of [`Self::remove_unbounded`].
    pub fn remove_bounded<T: BoundedOperative>(&self) -> Result<&Self, FlezhaError> {
        self.remove(T::factory_id(), true)
    }

    /// Connect to the controller. Blocks only for the connect handshake
    /// (retried with exponential backoff until the controller is reachable),
    /// never for the runner's ongoing lifetime. Claims the process-wide
    /// singleton slot by compare-and-set — at most one runner may be started
    /// per process; the loser fails with [`FlezhaError::AlreadyRunning`]
    /// WITHOUT connecting, mutating shared state, or spawning a thread.
    /// Starting with zero declared operative types is legal (the init frame
    /// carries an empty factory list; types may be defined later). Bind the
    /// runner when chaining — `let runner = Flezha::create_runner(ns)?.start()?;` —
    /// a chained temporary Drops (and shuts the runner down) at statement end.
    pub fn start(&self) -> Result<&Self, FlezhaError> {
        let mut started = self.core.started.lock().unwrap();
        if started.is_some() {
            return Err(FlezhaError::AlreadyRunning {
                namespace: self.core.namespace.clone(),
            });
        }

        // Claim the process-wide slot: exactly one concurrent start() wins,
        // and the loser must not connect or touch shared state.
        {
            let mut active = ACTIVE_RUNNER.lock().unwrap();
            if let Some(running) = active.as_ref() {
                return Err(FlezhaError::AlreadyRunning {
                    namespace: running.namespace.clone(),
                });
            }
            *active = Some(ActiveRunner {
                namespace: self.core.namespace.clone(),
                core: Arc::downgrade(&self.core),
            });
        }

        match self.connect() {
            Ok(live) => {
                *started = Some(live);
                Ok(self)
            }
            Err(e) => {
                *ACTIVE_RUNNER.lock().unwrap() = None;
                Err(e)
            }
        }
    }

    /// Sugar for standalone binaries: [`Self::start`] followed by
    /// [`Self::await_termination`]. Blocks the calling thread until the
    /// runner shuts down — no keep-alive loop needed in `main()`.
    pub fn run(&self) -> Result<(), FlezhaError> {
        self.start()?;
        self.await_termination()
    }

    /// Snapshot of the live UnboundedOperative-instance registry, keyed by
    /// what the wire calls `operative_id`. `Err(NotStarted)` on a
    /// never-started runner — never a panic.
    pub fn operatives(&self) -> Result<HashMap<String, Arc<UnboundedInstance>>, FlezhaError> {
        let started = self.core.started.lock().unwrap();
        let Some(live) = started.as_ref() else {
            return Err(FlezhaError::NotStarted);
        };

        match live.exchange(RunnerMessage::ReturnOperatives)? {
            ClientResponseMessage::ReturnOperatives(operatives) => Ok(operatives),
            _ => Err(FlezhaError::RunnerUnavailable(
                "unexpected reply from the runner thread".to_string(),
            )),
        }
    }

    /// Block the calling thread until the runner shuts down (the owning
    /// handle drops, or SIGTERM-triggered graceful shutdown exits the
    /// process). Replaces every hand-rolled keep-alive loop.
    /// `Err(NotStarted)` on a never-started runner — returns immediately.
    pub fn await_termination(&self) -> Result<(), FlezhaError> {
        if self.core.started.lock().unwrap().is_none() {
            return Err(FlezhaError::NotStarted);
        }
        let mut terminated = self.core.terminated.lock().unwrap();
        while !*terminated {
            terminated = self.core.terminated_cv.wait(terminated).unwrap();
        }
        Ok(())
    }

    /// Add `factory_id` to this runner's declared set and register the class
    /// globally, refusing a duplicate id; a post-start call ships the full
    /// set to the controller.
    fn declare(&self, factory_id: &str, register: impl FnOnce()) -> Result<(), FlezhaError> {
        {
            let mut declared = self.core.declared.write().unwrap();
            if declared.iter().any(|id| id == factory_id) {
                return Err(FlezhaError::DuplicateFactoryId {
                    factory_id: factory_id.to_string(),
                });
            }
            register();
            declared.push(factory_id.to_string());
        }
        self.sync_factories_if_started();
        Ok(())
    }

    /// Remove `factory_id` from this runner. Post-start, the live-instance
    /// check + class unregistration run on the coordinator loop — serialized
    /// with `AddOperative` handling by construction, so the check is
    /// race-free. Pre-start no instances can exist, so the class is
    /// unregistered directly.
    fn remove(&self, factory_id: &str, bounded: bool) -> Result<&Self, FlezhaError> {
        if !self
            .core
            .declared
            .read()
            .unwrap()
            .iter()
            .any(|id| id == factory_id)
        {
            return Err(FlezhaError::NotDefined {
                factory_id: factory_id.to_string(),
            });
        }

        {
            let started = self.core.started.lock().unwrap();
            match started.as_ref() {
                Some(live) => {
                    match live.exchange(RunnerMessage::RemoveFactory {
                        factory_id: factory_id.to_string(),
                        bounded,
                    })? {
                        ClientResponseMessage::RemoveFactoryResult(Ok(())) => {}
                        ClientResponseMessage::RemoveFactoryResult(Err(live_instances)) => {
                            return Err(FlezhaError::OperativeTypeInUse {
                                factory_id: factory_id.to_string(),
                                live_instances,
                            });
                        }
                        _ => {
                            return Err(FlezhaError::RunnerUnavailable(
                                "unexpected reply from the runner thread".to_string(),
                            ));
                        }
                    }
                }
                None => {
                    if bounded {
                        bounded::registry::unregister_class(factory_id);
                    } else {
                        operative::registry::unregister_class(factory_id);
                    }
                }
            }
        }

        self.core
            .declared
            .write()
            .unwrap()
            .retain(|id| id != factory_id);
        self.sync_factories_if_started();
        Ok(self)
    }

    /// Ship the runner's FULL current factory set to the controller after a
    /// post-start define/remove. Fire-and-forget (no waiter): full-set
    /// semantics make redelivery idempotent, and the reconnect init frame —
    /// built from the same `declared` set — remains the resync of record.
    /// A no-op on a never-started runner.
    fn sync_factories_if_started(&self) {
        let started = self.core.started.lock().unwrap();
        if let Some(live) = started.as_ref() {
            let factories = self
                .core
                .declared
                .read()
                .unwrap()
                .iter()
                .map(|id| pb::FactoryDecl {
                    id: id.clone(),
                    description: String::new(),
                })
                .collect();
            let _ = live.runner_tx.send(RunnerMessage::EmitControlRequest {
                reference_id: format!("update-factories-{}", crate::util::nonce_suffix()),
                command: pb::command_request::Command::UpdateFactories(pb::UpdateFactories {
                    factories,
                }),
            });
        }
    }

    fn connect(&self) -> Result<Started, FlezhaError> {
        // Harvest single-procedure auto-wire + `operative!` operatives into the
        // global registries and merge their ids into THIS runner's declared
        // set — on every start, so a restarted generation re-declares them.
        {
            let mut declared = self.core.declared.write().unwrap();
            for auto in inventory::iter::<AutoOperative> {
                (auto.register)();
                let factory_id = (auto.factory_id)();
                if !declared.iter().any(|id| id == factory_id) {
                    declared.push(factory_id.to_string());
                }
            }
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
            self.core.namespace.clone(),
            Arc::clone(&self.core.declared),
            (otx.clone(), orx),
            ctx,
            Arc::clone(&shutdown_bridge),
        );

        await_confirmation(&irx)?;

        Ok(Started {
            runner_tx: otx,
            client_rx: Mutex::new(irx),
            _runner_thread: runner_thread,
            shutdown_bridge,
        })
    }
}

/// Drop trigger for the graceful-shutdown sequence — started owning handle
/// only. An owner that never started performs no shutdown work, touches no
/// shared state, and releases no slot (none was claimed); observer handles
/// are always a no-op.
impl Drop for FlezhaRunner {
    fn drop(&mut self) {
        if !self.owner {
            return;
        }
        let started = self.core.started.lock().unwrap();
        let Some(live) = started.as_ref() else {
            return;
        };
        live.shutdown_bridge.run_graceful_shutdown();
        drop(started);
        *ACTIVE_RUNNER.lock().unwrap() = None;
        *self.core.terminated.lock().unwrap() = true;
        self.core.terminated_cv.notify_all();
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
            Err(FlezhaError::ConnectFailed(reason)) => {
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
