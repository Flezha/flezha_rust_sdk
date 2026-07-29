mod control_unit;

pub(crate) mod fast;
pub(crate) mod mailbox;
pub(crate) mod persistent_id;
pub(crate) mod work_stealing_pool;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use crossbeam::channel::Sender;

use crate::operative::instance::UnboundedInstance;

/// Process-global handle to the coordinator's message channel, installed
/// once by `fast::start`. Lets components outside the coordinator (e.g.
/// the segment-state sink on a broken-pipe flood) request a control
/// reconnect without threading the `Sender` through every layer.
static RECONNECT_TRIGGER: OnceLock<Mutex<Sender<RunnerMessage>>> = OnceLock::new();

pub(crate) fn install_reconnect_trigger(otx: Sender<RunnerMessage>) {
    // Replace on re-install: a runner restarted after a prior one closed must
    // not leave the trigger pointing at the dead coordinator's channel.
    match RECONNECT_TRIGGER.get() {
        Some(lock) => *lock.lock().unwrap() = otx,
        None => {
            let _ = RECONNECT_TRIGGER.set(Mutex::new(otx));
        }
    }
}

/// Send a message to the coordinator from outside its thread, so control- and
/// secondary-stream writes stay single-threaded. No-op when the trigger is not
/// installed (e.g. unit tests).
pub(crate) fn coordinator_send(msg: RunnerMessage) {
    let _ = coordinator_try_send(msg);
}

/// [`coordinator_send`] that reports whether the message actually reached a
/// coordinator: `false` when none is installed, and when the installed trigger
/// points at a channel nobody reads any more (a closed runner's stale sender).
/// Callers that would otherwise wait for a reply that can never come use this.
pub(crate) fn coordinator_try_send(msg: RunnerMessage) -> bool {
    match RECONNECT_TRIGGER.get() {
        Some(lock) => match lock.lock() {
            Ok(tx) => tx.send(msg).is_ok(),
            Err(_) => false,
        },
        None => false,
    }
}

/// Ask the coordinator to drive a control-channel reconnect. `reason` is one of
/// the canonical strings documented on [`RunnerMessage::Reconnect`].
pub(crate) fn request_reconnect(reason: &'static str) {
    coordinator_send(RunnerMessage::Reconnect(reason));
}

/// Whether a coordinator channel is installed — `false` only for
/// transport-less runs (unit tests, standalone benches), which cannot ship
/// control frames and must not block waiting for replies.
pub(crate) fn coordinator_installed() -> bool {
    RECONNECT_TRIGGER.get().is_some()
}

/// Route a bounded's output to the coordinator (written on a secondary stream
/// tagged with `operative_id`; the controller fans it out to the unit's plugins).
pub(crate) fn emit_bounded_output(operative_id: String, payload: Vec<u8>) {
    coordinator_send(RunnerMessage::EmitBoundedOutput {
        operative_id,
        payload,
    });
}

/// Route an SDK-initiated control request to the coordinator (written verbatim
/// under the caller's `reference_id`, so the controller's reply — an
/// `OpCommitResponse` / `IterCommitResponse` carrying the same id — wakes the
/// waiter the caller registered). Carries the bounded driver's op-boundary
/// commit, iteration commit, and park frames.
pub(crate) fn emit_control_request(
    reference_id: String,
    command: crate::pb::command_request::Command,
) {
    coordinator_send(RunnerMessage::EmitControlRequest {
        reference_id,
        command,
    });
}

/// Route a bounded's completion to the coordinator (written as a `BoundedCompleted`
/// control frame so the controller deregisters the unit). `result` is the
/// terminal result/error payload from `ctx.complete` (empty when none was set).
pub(crate) fn emit_bounded_completed(
    operative_id: String,
    status: String,
    error: String,
    result: Vec<u8>,
) {
    coordinator_send(RunnerMessage::EmitBoundedCompleted {
        operative_id,
        status,
        error,
        result,
    });
}

/// Serializes every test that mutates process-global runner state: the env
/// knobs (`FLEZHA_RUNNER_ID_PATH`, `FLEZHA_CONTROL_*`) and the coordinator
/// trigger a test installs to observe what the runtime routes through it. Both
/// are shared across the whole crate, so the lock lives here (not in one child
/// module) and is poison-tolerant: a failed assertion inside one holder must
/// not cascade-fail the unrelated tests that merely share the lock.
#[cfg(test)]
static ENV_TEST_LOCK: Mutex<()> = Mutex::new(());

#[cfg(test)]
pub(crate) fn env_test_guard() -> std::sync::MutexGuard<'static, ()> {
    ENV_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Messages routed through the coordinator channel. Data-plane traffic
/// bypasses this channel entirely (per-Segment mailboxes are pushed
/// directly by the stream readers); only control-plane traffic flows
/// through here.
pub enum RunnerMessage {
    HandleCommandAction(String, Vec<u8>, HashMap<String, String>),
    ReturnOperatives,
    /// Drive a reconnect with a structured reason for the observer log.
    /// The re-opened control stream re-presents the runner's stable id in
    /// its connect handshake (no separate reconnect frame). Reason is one of
    /// `control_stream_broken | read_error | write_error |
    /// keepalive_timeout | controller_requested | broken_pipe_flood`.
    Reconnect(&'static str),
    /// Tick from the keepalive timer: write a control-channel ping on the
    /// current control stream. Routed through the coordinator so control
    /// writes stay single-threaded (no interleave with command replies).
    SendControlKeepalive,
    /// Tick from the data-heartbeat timer: write a heartbeat frame on every
    /// secondary stream. Routed through the coordinator for the same
    /// single-threaded-write reason as `SendControlKeepalive`.
    SendDataHeartbeat,
    /// A bounded emitted output; the coordinator writes it on a secondary stream
    /// tagged with `operative_id` for the controller to fan out to the unit's plugins.
    EmitBoundedOutput {
        operative_id: String,
        payload: Vec<u8>,
    },
    /// A bounded reached an exit trigger; the coordinator writes a
    /// `BoundedCompleted` frame so the controller deregisters the unit.
    /// `result` carries the terminal result/error payload (empty when none).
    EmitBoundedCompleted {
        operative_id: String,
        status: String,
        error: String,
        result: Vec<u8>,
    },
    /// An SDK-initiated control request (the bounded driver's `OpCommit` /
    /// `IterCommit` / `IterPark`, or a post-start define/remove's
    /// `UpdateFactories`); the coordinator writes it verbatim under the
    /// caller's `reference_id`, which the controller echoes on its reply to wake
    /// the registered waiter (when one is registered — `UpdateFactories` is
    /// fire-and-forget). Routed here so control writes stay single-threaded.
    EmitControlRequest {
        reference_id: String,
        command: crate::pb::command_request::Command,
    },
    /// `FlezhaRunner::remove_unbounded` / `remove_bounded` on a started
    /// runner. Handled on the coordinator loop so the live-instance check is
    /// serialized with `AddOperative` handling by construction: either the
    /// add lands first and the removal refuses, or the removal lands first
    /// and the add fails as an unknown factory. Replies
    /// [`ClientResponseMessage::RemoveFactoryResult`].
    RemoveFactory {
        factory_id: String,
        bounded: bool,
    },
}

pub enum RunnerStatus {
    Starting,
    Running,
    Stopped,
}

pub enum ClientResponseMessage {
    RunnerIdConfirmation,
    /// Snapshot of live UnboundedOperative instances, returned to
    /// `FlezhaRunner::operatives()` callers.
    ReturnOperatives(HashMap<String, Arc<UnboundedInstance>>),
    /// Reply to [`RunnerMessage::RemoveFactory`]: `Ok(())` = the class was
    /// unregistered; `Err(n)` = `n` live instances exist and the type stays
    /// defined.
    RemoveFactoryResult(Result<(), usize>),
}

pub use fast::{start, RunnerNode};
