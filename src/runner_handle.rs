//! This process's runner handle: create → declare → start → drop.
//!
//! At most one runner is started per process. [`ACTIVE_RUNNER`] is that slot,
//! claimed by compare-and-set inside [`FlezhaRunner::start`] and released when
//! the started owning handle drops — so a second runner may start afterwards,
//! and an observer handle from [`Flezha::current_runner`] never shuts one down.

use std::sync::{Arc, Condvar, Mutex, Weak};

use flz_core::session::{Session, SessionError};

use crate::relay::Relay;
use crate::{shutdown, BuildStepsError, TopologyBuildError};

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
    /// No controller confirmed the connect within the start wait,
    /// `FLEZHA_RUNNER_START_WAIT_MS`.
    ConnectUnconfirmed { wait_ms: u64 },
    /// `register_factory` found an invalid topology (duplicate channel names,
    /// unwired channels, type mismatches, ...) — reported at the declaring
    /// call site, never a panic.
    InvalidTopology(TopologyBuildError),
    /// `register_factory` found invalid steps relay authoring.
    InvalidSteps(BuildStepsError),
    /// The factory id is already in this runner's declared set, whichever kind
    /// either relay is.
    DuplicateFactoryId { factory_id: String },
    /// `unregister_factory` named a factory that is not in this runner's declared set.
    NotDefined { factory_id: String },
    /// `unregister_factory` refused: live relay instances of the factory
    /// exist. The type remains defined.
    RelayTypeInUse {
        factory_id: String,
        live_instances: usize,
    },
    /// The call requires a started runner (`await_termination`, `relays`).
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
            FlezhaError::ConnectUnconfirmed { wait_ms } => E::with(
                "runtime.connect_unconfirmed",
                [("wait_ms", (*wait_ms).into())],
            ),
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
            FlezhaError::RelayTypeInUse {
                factory_id,
                live_instances,
            } => E::with(
                "runtime.relay_type_in_use",
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

flz_transport::model_error!(FlezhaError);

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

impl FlezhaError {
    fn of_session(e: SessionError) -> Self {
        match e {
            SessionError::DuplicateFactoryId(factory_id) => {
                FlezhaError::DuplicateFactoryId { factory_id }
            }
            SessionError::NotDefined(factory_id) => FlezhaError::NotDefined { factory_id },
            SessionError::RelayTypeInUse {
                factory_id,
                live_instances,
            } => FlezhaError::RelayTypeInUse {
                factory_id,
                live_instances,
            },
            SessionError::AlreadyStarted => FlezhaError::AlreadyRunning {
                namespace: String::new(),
            },
            SessionError::ConnectFailed(reason) => FlezhaError::ConnectFailed(reason.into()),
            SessionError::Refused(error) => FlezhaError::Refused(error),
            SessionError::Unconfirmed { wait_ms } => FlezhaError::ConnectUnconfirmed { wait_ms },
            SessionError::Unavailable(reason) => FlezhaError::RunnerUnavailable(reason.into()),
        }
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
/// follow `docs/internal/sdk/naming_conventions.md`, mirrored 1:1 with the Java SDK's
/// `com.flezha.sdk.Flezha`.
pub struct Flezha;

impl Flezha {
    /// Create this process's runner for `namespace` — one object carries
    /// declaration and lifecycle (Beam's `Pipeline.create()` shape). Declare
    /// relay classes of either kind with
    /// [`FlezhaRunner::register_factory`] (legal before AND after start), then
    /// [`FlezhaRunner::start`] (keep the calling thread) or
    /// [`FlezhaRunner::run`] (block until shutdown). Creating a runner never
    /// connects, claims the process singleton slot, or spawns a thread.
    ///
    /// ```no_run
    /// # use flezha_runner::{Flezha, FlezhaError};
    /// # struct MyRelay;
    /// # impl flezha_runner::Relay for MyRelay {
    /// #     fn factory_id() -> &'static str { "my_relay" }
    /// #     fn channels(_c: &flezha_runner::ChannelBuilder) {}
    /// # }
    /// # fn main() -> Result<(), FlezhaError> {
    /// Flezha::create_runner("my_app")?.register_factory(MyRelay)?.run()?;
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
                session: Session::default(),
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
    /// `relays()`, `await_termination()`); dropping it does NOT shut the
    /// runner down — only dropping the started owning handle does.
    pub fn current_runner() -> Option<Arc<FlezhaRunner>> {
        let active = ACTIVE_RUNNER.lock().unwrap();
        active
            .as_ref()
            .and_then(|a| a.core.upgrade())
            .map(|core| Arc::new(FlezhaRunner { core, owner: false }))
    }
}

/// State shared between the owning handle [`Flezha::create_runner`] returned
/// and observer handles from [`Flezha::current_runner`]: the session, whose
/// factory set is per-runner and empty at creation, and the termination latch.
struct RunnerCore {
    namespace: String,
    session: Session,
    terminated: Mutex<bool>,
    terminated_cv: Condvar,
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

    /// The message's id, stamped as `flz.msg_id`: a relay whose input log
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

    /// Hand the message to a relay bound on this runner, in process,
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
    /// stop relays, enable and disable plugins, read what is running.
    ///
    /// The controller judges every call as the identity this connection
    /// proved, so no credential is held here and nothing expires. A user
    /// grants a namespace's applications the authority once, with
    /// `flz role bind runner@<namespace> app <namespace>`; until then every
    /// call is refused. See [`crate::control`].
    pub fn control(&self) -> crate::control::Control {
        crate::control::Control
    }

    /// Send `message` to the entry channel `channel` of `relay_id`, through
    /// the controller under the same role check as [`Self::control`], and
    /// answer its verdict: `{"to": <relay>, "verdict": "delivered" |
    /// "absorbed" | "suspended"}`. [`crate::SendOpts`] adds a message id or a
    /// partition key, or makes the send local: in process to a relay
    /// bound on this runner, answering null, with no durability at all.
    pub fn send(
        &self,
        relay_id: &str,
        message: &str,
        channel: &str,
        opts: &SendOpts,
    ) -> Result<serde_json::Value, crate::ControlError> {
        let answer = flz_core::admin::send(
            relay_id,
            message.as_bytes(),
            channel,
            opts.msg_id.as_deref(),
            opts.key.as_deref(),
            opts.local,
        )?;
        crate::control::read("Relay.send", &answer)
    }

    /// Call the entry channel `channel` of `relay_id` with `value`, by the
    /// relay's API (`Relay.call_channel`), under the role check
    /// [`Self::send`] takes. `value` is encoded as an output with no
    /// serializer encodes it: `Vec<u8>` as it is, a `String` as its UTF-8,
    /// anything else as JSON. The answer is read into `A` by its format: JSON
    /// as JSON, text as a string, bytes as a byte array, and
    /// `{"accepted": true}` for a channel that does not answer.
    pub fn call<V, A>(
        &self,
        relay_id: &str,
        channel: &str,
        value: &V,
    ) -> Result<A, crate::ControlError>
    where
        V: serde::Serialize + Send + Sync + 'static,
        A: serde::de::DeserializeOwned,
    {
        let payload = crate::channel::builder::default_plugin_serializer::<V>()(value);
        let answer = flz_core::admin::call_channel(relay_id, channel, &payload)?;
        let answer: serde_json::Value = crate::control::read("Relay.call_channel", &answer)?;
        let value = match (answer["answer_codec"].as_str(), &answer["answer"]) {
            (Some("json" | "text"), value) => value.clone(),
            (Some(_), serde_json::Value::String(base64)) => flz_core::call::base64_decode(base64)
                .map_or(serde_json::Value::Null, |bytes| {
                    serde_json::Value::from(bytes)
                }),
            _ => answer,
        };
        crate::control::read("Relay.call_channel", &value.to_string())
    }

    /// Declare a relay class the runtime can spawn instances of later
    /// (via the controller's `AddRelay`). Builds the class once, under its
    /// [`Relay::settings`], and records it in the global class registry.
    /// Legal before and after `start()`; a post-start registration ships the
    /// runner's full factory set to the controller (`UpdateFactories`).
    ///
    /// An invalid declaration (duplicate channel names, unwired channels,
    /// missing `cells_for` overrides, ...) is reported here, at the declaring
    /// call site — not a panic — and leaves the runner exactly as it was.
    ///
    /// The relay is passed as a value so a call reads as the class it
    /// declares; relays are unit structs, so the value costs nothing.
    pub fn register_factory<O: Relay>(&self, _relay: O) -> Result<&Self, FlezhaError> {
        // Build/validate BEFORE touching the declared set.
        let class = crate::relay::build_class::<O>()?;
        let register = || {
            crate::relay::registry::register_class(O::factory_id(), class);
            Ok(())
        };
        (self.core.session)
            .declare(O::factory_id(), register)
            .map_err(FlezhaError::of_session)?;
        Ok(self)
    }

    /// Remove a relay class from this runner — the inverse of
    /// [`Self::register_factory`], taking what it took. Legal before and after
    /// `start()`. Refuses with [`FlezhaError::RelayTypeInUse`] while live
    /// instances of the factory exist (the type stays defined) and with
    /// [`FlezhaError::NotDefined`] for a factory this runner never declared.
    /// A post-start removal ships the remaining full factory set to the
    /// controller (`UpdateFactories`).
    pub fn unregister_factory<O: Relay>(&self, _relay: O) -> Result<&Self, FlezhaError> {
        (self.core.session)
            .undeclare(O::factory_id())
            .map_err(FlezhaError::of_session)?;
        Ok(self)
    }

    /// Connect to the controller. Blocks only for the connect handshake
    /// (retried with exponential backoff), never for the runner's ongoing
    /// lifetime, and for at most `FLEZHA_RUNNER_START_WAIT_MS` (10 s by
    /// default, `0` for ever): past it the start fails with
    /// [`FlezhaError::ConnectUnconfirmed`] and leaves no runner dialling. Claims the process-wide
    /// singleton slot by compare-and-set — at most one runner may be started
    /// per process; the loser fails with [`FlezhaError::AlreadyRunning`]
    /// WITHOUT connecting, mutating shared state, or spawning a thread.
    /// Starting with zero declared relay types is legal (the init frame
    /// carries an empty factory list; types may be declared later), whatever
    /// short-form or `pipeline!` types the binary happens to contain: nothing
    /// is harvested here. Bind the runner before starting it
    /// (`let runner = Flezha::create_runner(ns)?; runner.start()?;`): a
    /// chained temporary Drops (and shuts the runner down) at statement end.
    pub fn start(&self) -> Result<&Self, FlezhaError> {
        let namespace = || self.namespace().to_string();
        if self.core.session.is_started() {
            return Err(FlezhaError::AlreadyRunning {
                namespace: namespace(),
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
                namespace: namespace(),
                core: Arc::downgrade(&self.core),
            });
        }

        (self.core.session)
            .start(&self.core.namespace, Vec::new())
            .map_err(|e| {
                *ACTIVE_RUNNER.lock().unwrap() = None;
                FlezhaError::of_session(e)
            })?;
        Ok(self)
    }

    /// Sugar for standalone binaries: [`Self::start`] followed by
    /// [`Self::await_termination`]. Blocks the calling thread until the
    /// runner shuts down — no keep-alive loop needed in `main()`.
    pub fn run(&self) -> Result<(), FlezhaError> {
        self.start()?;
        self.await_termination()
    }

    /// The ids of the relay instances bound on this runner now, a
    /// partition member by its member id, read without the controller.
    /// `Err(NotStarted)` on a never-started runner — never a panic.
    pub fn relays(&self) -> Result<Vec<String>, FlezhaError> {
        if !self.core.session.is_started() {
            return Err(FlezhaError::NotStarted);
        }
        Ok(flz_core::runner::bound().ids())
    }

    /// Block the calling thread until the runner shuts down (the owning
    /// handle drops, or SIGTERM-triggered graceful shutdown exits the
    /// process). Replaces every hand-rolled keep-alive loop.
    /// `Err(NotStarted)` on a never-started runner — returns immediately.
    pub fn await_termination(&self) -> Result<(), FlezhaError> {
        if !self.core.session.is_started() {
            return Err(FlezhaError::NotStarted);
        }
        let mut terminated = self.core.terminated.lock().unwrap();
        while !*terminated {
            terminated = self.core.terminated_cv.wait(terminated).unwrap();
        }
        Ok(())
    }
}

/// Drop trigger for the graceful-shutdown sequence — started owning handle
/// only. An owner that never started performs no shutdown work, touches no
/// shared state, and releases no slot (none was claimed); observer handles
/// are always a no-op.
impl Drop for FlezhaRunner {
    fn drop(&mut self) {
        if !self.owner || !self.core.session.is_started() {
            return;
        }
        shutdown::exit(shutdown::Exit::default());
        *ACTIVE_RUNNER.lock().unwrap() = None;
        *self.core.terminated.lock().unwrap() = true;
        self.core.terminated_cv.notify_all();
    }
}
