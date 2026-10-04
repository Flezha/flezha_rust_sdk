//! This process's runner handle: create → declare → start → drop.
//!
//! At most one runner is started per process. [`ACTIVE_RUNNER`] is that slot,
//! claimed by compare-and-set inside [`FlezhaRunner::start`] and released when
//! the started owning handle drops — so a second runner may start afterwards,
//! and an observer handle from [`Flezha::current_runner`] never shuts one down.

use std::sync::{Arc, Condvar, Mutex, RwLock, Weak};

use crossbeam::channel::{Receiver, Sender};

use crate::operator::Operator;
use crate::runner::{ClientResponseMessage, RunnerMessage};
use crate::shutdown::ShutdownBridge;
use crate::{operator, pb, runner, shutdown, BuildStepsError, TopologyBuildError};

/// The ONE public error of the entry-point chain: every fallible call on
/// [`FlezhaRunner`] returns `Result<_, FlezhaError>`, so
/// `Flezha::create_runner(ns)?.register_factory(A)?.run()?` compiles
/// under a single `?`. Its [`ErrorModel`] accessors carry the catalogue code a
/// program branches on; `Display` is the catalogue message.
#[derive(Debug)]
pub enum FlezhaError {
    /// [`Flezha::create_runner`] was given an empty namespace.
    InvalidNamespace,
    /// The process-wide singleton guard: a runner is already started in this
    /// process (or `.start()` was called twice on one runner). Close (drop)
    /// it before starting another.
    AlreadyRunning { namespace: String },
    /// The connect handshake failed: the runner thread exited before
    /// delivering its connection confirmation.
    ConnectFailed(String),
    /// The controller refused the connect, under its own code.
    Refused(flz_core::FlezhaError),
    /// `register_factory` found an invalid topology (duplicate channel names,
    /// unwired channels, type mismatches, ...) — reported at the declaring
    /// call site, never a panic.
    InvalidTopology(TopologyBuildError),
    /// `register_factory` found invalid steps operator authoring.
    InvalidSteps(BuildStepsError),
    /// The factory id is already in this runner's declared set, whichever kind
    /// either operator is.
    DuplicateFactoryId { factory_id: String },
    /// `unregister_factory` named a factory that is not in this runner's declared set.
    NotDefined { factory_id: String },
    /// `unregister_factory` refused: live operator instances of the factory
    /// exist. The type remains defined.
    OperatorTypeInUse {
        factory_id: String,
        live_instances: usize,
    },
    /// The call requires a started runner (`await_termination`, `operators`).
    NotStarted,
    /// The runner thread is gone or replied with an unexpected message.
    RunnerUnavailable(String),
    /// A local run the core refused: an unregistered class, a declaration
    /// that needs a controller, or a channel name nothing matches.
    Local(flz_core::Error),
}

pub use flz_core::ErrorModel;

impl ErrorModel for FlezhaError {
    fn to_error(&self) -> flz_core::FlezhaError {
        use flz_core::FlezhaError as E;
        match self {
            FlezhaError::InvalidNamespace => {
                E::with("namespace.invalid_name", [("namespace", "".into())])
            }
            FlezhaError::AlreadyRunning { namespace } => E::with(
                "runtime.runner_already_running",
                [("namespace", namespace.as_str().into())],
            ),
            FlezhaError::ConnectFailed(reason) => E::with(
                "runner.controller_unreachable",
                [("reason", reason.as_str().into())],
            ),
            FlezhaError::Refused(error) => error.clone(),
            FlezhaError::InvalidTopology(e) => e.to_error(),
            FlezhaError::InvalidSteps(e) => e.to_error(),
            FlezhaError::DuplicateFactoryId { factory_id } => E::with(
                "topology.duplicate_factory_id",
                [("factory_id", factory_id.as_str().into())],
            ),
            FlezhaError::NotDefined { factory_id } => E::with(
                "runtime.class_not_registered",
                [("factory", factory_id.as_str().into())],
            ),
            FlezhaError::OperatorTypeInUse {
                factory_id,
                live_instances,
            } => E::with(
                "runtime.operator_type_in_use",
                [
                    ("factory_id", factory_id.as_str().into()),
                    ("live_instances", (*live_instances).into()),
                ],
            ),
            FlezhaError::NotStarted => E::with("runtime.runner_not_started", []),
            FlezhaError::RunnerUnavailable(reason) => E::with(
                "runtime.runner_unavailable",
                [("reason", reason.as_str().into())],
            ),
            FlezhaError::Local(e) => e.to_error(),
        }
    }
}

impl ErrorModel for TopologyBuildError {
    fn to_error(&self) -> flz_core::FlezhaError {
        flz_core::host::render_issues(&self.errors)
    }
}

impl std::fmt::Display for FlezhaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_error().message)
    }
}

impl std::error::Error for FlezhaError {}

impl From<TopologyBuildError> for FlezhaError {
    fn from(e: TopologyBuildError) -> Self {
        FlezhaError::InvalidTopology(e)
    }
}

impl From<BuildStepsError> for FlezhaError {
    fn from(e: BuildStepsError) -> Self {
        FlezhaError::InvalidSteps(e)
    }
}

impl From<flz_core::Error> for FlezhaError {
    fn from(e: flz_core::Error) -> Self {
        FlezhaError::Local(e)
    }
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
/// follow `docs/sdk/naming_conventions.md`, mirrored 1:1 with the Java SDK's
/// `com.flezha.sdk.Flezha`.
pub struct Flezha;

impl Flezha {
    /// Create this process's runner for `namespace` — one object carries
    /// declaration and lifecycle (Beam's `Pipeline.create()` shape). Declare
    /// operator classes of either kind with
    /// [`FlezhaRunner::register_factory`] (legal before AND after start), then
    /// [`FlezhaRunner::start`] (keep the calling thread) or
    /// [`FlezhaRunner::run`] (block until shutdown). Creating a runner never
    /// connects, claims the process singleton slot, or spawns a thread.
    ///
    /// ```no_run
    /// # use flezha_runner::{Flezha, FlezhaError};
    /// # struct MyOperator;
    /// # impl flezha_runner::Operator for MyOperator {
    /// #     fn factory_id() -> &'static str { "my_operator" }
    /// #     fn channels(_c: &flezha_runner::ChannelBuilder) {}
    /// # }
    /// # fn main() -> Result<(), FlezhaError> {
    /// Flezha::create_runner("my_app")?.register_factory(MyOperator)?.run()?;
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
    /// `operators()`, `await_termination()`); dropping it does NOT shut the
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
        Ok(ClientResponseMessage::ConnectRefused(error)) => Err(FlezhaError::Refused(error)),
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

    /// The reply an exchange cannot belong to: the runner thread answered with
    /// a message that is not this request's. One text, both round trips.
    fn unexpected_reply() -> FlezhaError {
        FlezhaError::RunnerUnavailable("unexpected reply from the runner thread".to_string())
    }
}

/// How a [`FlezhaRunner::send`](crate::FlezhaRunner::send) travels: through
/// the controller, unless [`Self::local`] says otherwise.
#[derive(Debug, Clone, Default)]
pub struct SendOpts {
    msg_id: Option<String>,
    key: Option<String>,
    local: bool,
}

impl SendOpts {
    /// Through the controller, with no id and no key.
    pub fn new() -> Self {
        Self::default()
    }

    /// The message's id, stamped as `flz.msg_id`: an operator whose input log
    /// still holds an input of this id absorbs the repeat and answers
    /// `absorbed`.
    pub fn msg_id(mut self, id: impl Into<String>) -> Self {
        self.msg_id = Some(id.into());
        self
    }

    /// The partition key: the member of a partition group whose range owns
    /// it takes the message.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Hand the message to an operator bound on this runner, in process,
    /// without the controller: it is on no disk, is lost if the runner stops
    /// before handling it, and an input log's guarantee does not cover it.
    pub fn local(mut self) -> Self {
        self.local = true;
        self
    }
}

/// This process's runner: create → declare (`register_factory` /
/// `unregister_factory`) → start
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

    /// Administer this namespace from ordinary application code: start and
    /// stop operators, enable and disable plugins, read what is running.
    ///
    /// The controller judges every call as the identity this connection
    /// proved, so no credential is held here and nothing expires. A user
    /// grants a namespace's applications the authority once, with
    /// `flz role bind runner@<namespace> app <namespace>`; until then every
    /// call is refused. See [`crate::control`].
    pub fn control(&self) -> crate::control::Control {
        crate::control::Control
    }

    /// Send `message` to the entry channel `channel` of `operator_id`, through
    /// the controller under the same role check as [`Self::control`], and
    /// answer its verdict: `{"to": <operator>, "verdict": "delivered" |
    /// "absorbed" | "suspended"}`. [`crate::SendOpts`] adds a message id or a
    /// partition key, or makes the send local: in process to an operator
    /// bound on this runner, answering null, with no durability at all.
    pub fn send(
        &self,
        operator_id: &str,
        message: &str,
        channel: &str,
        opts: &SendOpts,
    ) -> Result<serde_json::Value, crate::ControlError> {
        let answer = flz_core::admin::send(
            operator_id,
            message.as_bytes(),
            channel,
            opts.msg_id.as_deref(),
            opts.key.as_deref(),
            opts.local,
        )?;
        crate::control::read("Operator.send", &answer)
    }

    /// Declare an operator class the runtime can spawn instances of later
    /// (via the controller's `AddOperator`). Builds the class once, under its
    /// [`Operator::settings`], and records it in the global class registry.
    /// Legal before and after `start()`; a post-start registration ships the
    /// runner's full factory set to the controller (`UpdateFactories`).
    ///
    /// An invalid declaration (duplicate channel names, unwired channels,
    /// missing `cells_for` overrides, ...) is reported here, at the declaring
    /// call site — not a panic — and leaves the runner exactly as it was.
    ///
    /// The operator is passed as a value so a call reads as the class it
    /// declares; operators are unit structs, so the value costs nothing.
    pub fn register_factory<O: Operator>(&self, _operator: O) -> Result<&Self, FlezhaError> {
        // Build/validate BEFORE touching the declared set.
        let class = crate::operator::build_class::<O>()?;
        self.declare(O::factory_id(), || {
            crate::operator::registry::register_class(O::factory_id(), class)
        })?;
        Ok(self)
    }

    /// Remove an operator class from this runner — the inverse of
    /// [`Self::register_factory`], taking what it took. Legal before and after
    /// `start()`. Refuses with [`FlezhaError::OperatorTypeInUse`] while live
    /// instances of the factory exist (the type stays defined) and with
    /// [`FlezhaError::NotDefined`] for a factory this runner never declared.
    /// A post-start removal ships the remaining full factory set to the
    /// controller (`UpdateFactories`).
    pub fn unregister_factory<O: Operator>(
        &self,
        _operator: O,
    ) -> Result<&Self, FlezhaError> {
        self.remove(O::factory_id())
    }

    /// Connect to the controller. Blocks only for the connect handshake
    /// (retried with exponential backoff until the controller is reachable),
    /// never for the runner's ongoing lifetime. Claims the process-wide
    /// singleton slot by compare-and-set — at most one runner may be started
    /// per process; the loser fails with [`FlezhaError::AlreadyRunning`]
    /// WITHOUT connecting, mutating shared state, or spawning a thread.
    /// Starting with zero declared operator types is legal (the init frame
    /// carries an empty factory list; types may be declared later), whatever
    /// short-form or `pipeline!` types the binary happens to contain: nothing
    /// is harvested here. Bind the runner before starting it
    /// (`let runner = Flezha::create_runner(ns)?; runner.start()?;`): a
    /// chained temporary Drops (and shuts the runner down) at statement end.
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

    /// The ids of the operator instances bound on this runner now, a
    /// partition member by its member id, read without the controller.
    /// `Err(NotStarted)` on a never-started runner — never a panic.
    pub fn operators(&self) -> Result<Vec<String>, FlezhaError> {
        if self.core.started.lock().unwrap().is_none() {
            return Err(FlezhaError::NotStarted);
        }
        Ok(flz_core::runner::bound().ids())
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
    /// with `AddOperator` handling by construction, so the check is
    /// race-free. Pre-start no instances can exist, so the class is
    /// unregistered directly.
    fn remove(&self, factory_id: &str) -> Result<&Self, FlezhaError> {
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
                Some(live) => refuse_live_instances(live, factory_id)?,
                None => operator::registry::unregister_class(factory_id),
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
    /// post-start registration or removal. Fire-and-forget (no waiter): full-set
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
                .map(|id| flz_core::operator::registry::advertised_for_factory(id))
                .collect();
            let _ = live.runner_tx.send(RunnerMessage::control_request(
                format!("update-factories-{}", crate::util::nonce_suffix()),
                pb::command_request::Command::UpdateFactories(pb::UpdateFactories { factories }),
            ));
        }
    }

    fn connect(&self) -> Result<Started, FlezhaError> {
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

/// Post-start removal: the live-instance check and the class unregistration
/// run on the coordinator loop, serialized with `AddOperator` handling by
/// construction, so the check is race-free. Refuses while instances exist.
fn refuse_live_instances(live: &Started, factory_id: &str) -> Result<(), FlezhaError> {
    match live.exchange(RunnerMessage::RemoveFactory {
        factory_id: factory_id.to_string(),
    })? {
        ClientResponseMessage::RemoveFactoryResult(Ok(())) => Ok(()),
        ClientResponseMessage::RemoveFactoryResult(Err(live_instances)) => {
            Err(FlezhaError::OperatorTypeInUse {
                factory_id: factory_id.to_string(),
                live_instances,
            })
        }
        _ => Err(Started::unexpected_reply()),
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
