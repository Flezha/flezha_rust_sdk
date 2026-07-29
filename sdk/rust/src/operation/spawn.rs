//! Operation-requested spawn of a bounded operative
//! (`add-operation-requested-spawn`).
//!
//! [`OperationCtx::spawn_bounded`](crate::operation::OperationCtx::spawn_bounded)
//! ships one `SpawnBoundedRequest` on the control plane and hands the caller a
//! [`SpawnHandle`]. The request never blocks the operation and its failure is
//! never the operation's failure: everything the controller decides comes back
//! through the handle, and a handle nobody looks at reports a failure to the
//! log instead of vanishing.

use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::OnceLock;
use std::time::Duration;

use crate::shutdown::AckResult;

/// How long a spawn result is awaited before it is reported as timed out.
/// Generous: the controller may have to start an instance, which waits for a
/// runner advertising the factory.
pub const SPAWN_RESULT_TIMEOUT: Duration = Duration::from_secs(30);

/// What to do when the requested `instance_id` is already running.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IfRunning {
    /// Deliver the payload to the running instance's inbox.
    #[default]
    Deliver,
    /// Leave the running instance alone and report
    /// [`SpawnError::AlreadyRunning`].
    Error,
}

impl IfRunning {
    pub(crate) fn as_wire(self) -> &'static str {
        match self {
            IfRunning::Deliver => "deliver",
            IfRunning::Error => "error",
        }
    }
}

/// A spawn request that reached an instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpawnOutcome {
    /// The id was free: a fresh instance was started with the payload as its
    /// first input.
    Spawned,
    /// The id was taken: the payload became one more input of the running
    /// instance.
    Delivered,
    /// The invocation was a replay of already-processed input, so no request
    /// was issued. Spawning is a side effect of live processing only.
    SkippedOnReplay,
}

/// Why a spawn request reached no instance. None of these fault the requesting
/// operation — they resolve its handle.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SpawnError {
    #[error("an instance of `{0}` is already running and if_running was Error")]
    AlreadyRunning(String),

    #[error("no connected runner registers factory `{0}`")]
    FactoryUnknown(String),

    #[error("the controller refused the spawn: {0}")]
    Refused(String),

    #[error("no spawn result within {}s", SPAWN_RESULT_TIMEOUT.as_secs())]
    Timeout,

    #[error("no control plane to request a spawn on")]
    Unavailable,
}

/// The async result of a [`spawn_bounded`](crate::operation::OperationCtx::spawn_bounded)
/// request. Observing it is optional: a handle dropped before its result
/// arrives keeps waiting in the background and logs a warning if the request
/// failed, so a fire-and-forget requester never loses the diagnosis.
#[must_use = "a SpawnHandle reports the spawn's outcome; drop it deliberately if you \
              only want failures logged"]
pub struct SpawnHandle {
    instance_id: String,
    factory_id: String,
    state: State,
}

/// A handle is waiting for a reply, already holds an unread answer, or has
/// handed that answer to the caller. One field, no impossible combinations.
enum State {
    Pending(Receiver<AckResult>),
    Unread(Result<SpawnOutcome, SpawnError>),
    Read,
}

impl SpawnHandle {
    pub(crate) fn pending(
        instance_id: String,
        factory_id: String,
        rx: Receiver<AckResult>,
    ) -> Self {
        Self {
            instance_id,
            factory_id,
            state: State::Pending(rx),
        }
    }

    /// A handle that already knows its answer — nothing was sent.
    pub(crate) fn resolved(
        instance_id: String,
        factory_id: String,
        result: Result<SpawnOutcome, SpawnError>,
    ) -> Self {
        Self {
            instance_id,
            factory_id,
            state: State::Unread(result),
        }
    }

    /// Block for the controller's answer, up to [`SPAWN_RESULT_TIMEOUT`].
    /// Consumes the handle, so the drop-time warning never double-reports what
    /// the caller has seen.
    pub fn wait(mut self) -> Result<SpawnOutcome, SpawnError> {
        match std::mem::replace(&mut self.state, State::Read) {
            State::Pending(rx) => interpret(
                &self.instance_id,
                &self.factory_id,
                rx.recv_timeout(SPAWN_RESULT_TIMEOUT),
            ),
            State::Unread(result) => result,
            State::Read => Err(SpawnError::Timeout),
        }
    }

    /// The answer if it has already arrived, without blocking. `None` leaves
    /// the handle pending, so a later `wait` (or the drop-time warning) still
    /// sees the result.
    pub fn try_result(&mut self) -> Option<Result<SpawnOutcome, SpawnError>> {
        let answer = match &self.state {
            State::Pending(rx) => match rx.try_recv() {
                Ok(reply) => interpret(&self.instance_id, &self.factory_id, Ok(reply)),
                Err(mpsc::TryRecvError::Empty) => return None,
                Err(mpsc::TryRecvError::Disconnected) => Err(SpawnError::Timeout),
            },
            State::Unread(_) => match std::mem::replace(&mut self.state, State::Read) {
                State::Unread(result) => result,
                _ => unreachable!("state was just observed as Unread"),
            },
            State::Read => return None,
        };
        self.state = State::Read;
        Some(answer)
    }
}

impl Drop for SpawnHandle {
    fn drop(&mut self) {
        match std::mem::replace(&mut self.state, State::Read) {
            State::Pending(rx) => watch(self.instance_id.clone(), self.factory_id.clone(), rx),
            State::Unread(Err(e)) => warn(&self.instance_id, &self.factory_id, &e),
            _ => {}
        }
    }
}

/// Map a controller reply — or the absence of one — onto the public result.
/// `already_running` / `factory_unknown` arrive as outcomes rather than errors
/// because they are the controller's decision, not a failed exchange.
fn interpret(
    instance_id: &str,
    factory_id: &str,
    reply: Result<AckResult, RecvTimeoutError>,
) -> Result<SpawnOutcome, SpawnError> {
    match reply {
        Ok(Ok(outcome)) => match outcome.as_str() {
            "spawned" => Ok(SpawnOutcome::Spawned),
            "delivered" => Ok(SpawnOutcome::Delivered),
            "already_running" => Err(SpawnError::AlreadyRunning(instance_id.to_string())),
            "factory_unknown" => Err(SpawnError::FactoryUnknown(factory_id.to_string())),
            other => Err(SpawnError::Refused(format!("unknown outcome `{other}`"))),
        },
        Ok(Err(error)) => Err(SpawnError::Refused(error)),
        Err(_) => Err(SpawnError::Timeout),
    }
}

fn warn(instance_id: &str, factory_id: &str, error: &SpawnError) {
    eprintln!("[flezha] spawn_bounded({instance_id}, {factory_id}) failed: {error}");
}

/// Hand an unobserved handle's receiver to the reaper so its failure still
/// reaches the log. One thread for the whole process, started on the first
/// dropped handle — a requester that never inspects its handles must not pay a
/// thread per request.
fn watch(instance_id: String, factory_id: String, rx: Receiver<AckResult>) {
    static REAPER: OnceLock<Sender<(String, String, Receiver<AckResult>)>> = OnceLock::new();
    let tx = REAPER.get_or_init(|| {
        let (tx, work): (Sender<(String, String, Receiver<AckResult>)>, _) = mpsc::channel();
        std::thread::Builder::new()
            .name("flz-spawn-results".to_string())
            .spawn(move || {
                for (instance_id, factory_id, rx) in work {
                    if let Err(e) = interpret(
                        &instance_id,
                        &factory_id,
                        rx.recv_timeout(SPAWN_RESULT_TIMEOUT),
                    ) {
                        warn(&instance_id, &factory_id, &e);
                    }
                }
            })
            .expect("spawn the flezha spawn-result reaper thread");
        tx
    });
    let _ = tx.send((instance_id, factory_id, rx));
}

/// Ship the request and hand back its pending handle. A run with nothing
/// listening on the control plane — a transport-less unit test or bench, or a
/// closed runner — resolves immediately rather than waiting out the deadline
/// for a reply that cannot come.
pub(crate) fn request(
    instance_id: String,
    factory_id: String,
    payload: Vec<u8>,
    if_running: IfRunning,
) -> SpawnHandle {
    let reference_id = format!(
        "spawn-bounded-{}-{}",
        instance_id,
        crate::util::nonce_suffix()
    );
    // Register before sending: the reply can then never arrive before its
    // waiter exists (same ordering rule as the op-commit barrier).
    let rx = crate::shutdown::register_pending_ack(reference_id.clone());

    let sent =
        crate::runner::coordinator_try_send(crate::runner::RunnerMessage::EmitControlRequest {
            reference_id: reference_id.clone(),
            command: crate::pb::command_request::Command::SpawnBoundedRequest(
                crate::pb::SpawnBoundedRequest {
                    instance_id: instance_id.clone(),
                    factory_id: factory_id.clone(),
                    payload,
                    if_running: if_running.as_wire().to_string(),
                },
            ),
        });

    if !sent {
        crate::shutdown::drop_pending_ack(&reference_id);
        return SpawnHandle::resolved(instance_id, factory_id, Err(SpawnError::Unavailable));
    }

    SpawnHandle::pending(instance_id, factory_id, rx)
}
