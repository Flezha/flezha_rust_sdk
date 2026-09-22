//! The controller half a workflow run talks to: what a barrier send, a
//! journal entry and a lost ack look like from the runner's side, plus the
//! drains that turn `RunnerMessage`s into a scenario's observations.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam::channel::Receiver;
use flz_core::runner::RunnerNode;

use crate::runner::RunnerMessage;

/// Task-commits observed by the fake controller (`workflow-task-journal`
/// barrier): every `TaskCommit` the drain helpers ack is recorded here
/// so the gating/handoff assertions can inspect what would be journaled.
pub(crate) static TASK_COMMITS: Mutex<Vec<crate::pb::TaskCommit>> = Mutex::new(Vec::new());

/// Iteration commits observed by the fake controller (`workflow-durable-loop`
/// boundary) — what the journal would record as `iter_committed`.
pub(crate) static ITER_COMMITS: Mutex<Vec<crate::pb::IterCommit>> = Mutex::new(Vec::new());

/// Park notifications observed by the fake controller
/// (`workflow-durable-suspension`) — the instance evicted itself at a
/// committed boundary.
pub(crate) static ITER_PARKS: Mutex<Vec<crate::pb::IterPark>> = Mutex::new(Vec::new());

/// Every barrier send the fake controller saw, `(operative_id,
/// reference_id)`, re-sends included.
pub(crate) static BARRIER_SENDS: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

/// Reference ids the fake controller has journaled: a re-send under one of
/// them is acked from the existing entry, never recorded twice.
pub(crate) static JOURNALED_REFS: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Per operative, how many barriers lose their first ack on the way back
/// (`usize::MAX` = every send of every barrier). A lost ack is the real
/// controller's: the entry is journaled and its outbox flushed, only the
/// reply never reaches the runner.
pub(crate) static LOST_ACKS: Mutex<Vec<(String, usize)>> = Mutex::new(Vec::new());

/// Every `EmitWorkflowOutput` the drain helpers saw — output that went
/// STRAIGHT to the wire, bypassing any task boundary. A durable structured
/// operative must produce none of these (it stages); an opted-out class
/// still does.
pub(crate) static WIRE_EMITS: Mutex<Vec<(String, Vec<u8>)>> = Mutex::new(Vec::new());

pub(crate) fn drain_task_commits() -> Vec<crate::pb::TaskCommit> {
    std::mem::take(&mut *TASK_COMMITS.lock().unwrap())
}

pub(crate) fn drain_iter_commits() -> Vec<crate::pb::IterCommit> {
    std::mem::take(&mut *ITER_COMMITS.lock().unwrap())
}

pub(crate) fn drain_iter_parks() -> Vec<crate::pb::IterPark> {
    std::mem::take(&mut *ITER_PARKS.lock().unwrap())
}

pub(crate) fn drain_barrier_sends() -> Vec<(String, String)> {
    std::mem::take(&mut *BARRIER_SENDS.lock().unwrap())
}

/// The reference ids of `operative_id`'s barrier sends, in order.
pub(crate) fn barrier_sends_of(operative_id: &str) -> Vec<String> {
    BARRIER_SENDS
        .lock()
        .unwrap()
        .iter()
        .filter(|(id, _)| id == operative_id)
        .map(|(_, reference_id)| reference_id.clone())
        .collect()
}

pub(crate) fn lose_acks(operative_id: &str, count: usize) {
    LOST_ACKS
        .lock()
        .unwrap()
        .push((operative_id.to_string(), count));
}

/// Whether this send's ack is lost: a barrier's first send consumes one of
/// the operative's losses, a re-send is acked — unless every ack is lost.
pub(crate) fn ack_lost(operative_id: &str, first_send: bool) -> bool {
    let mut lost = LOST_ACKS.lock().unwrap();
    match lost.iter_mut().find(|(id, _)| id == operative_id) {
        Some((_, n)) if *n == usize::MAX => true,
        Some((_, n)) if *n > 0 && first_send => {
            *n -= 1;
            true
        }
        _ => false,
    }
}

/// The controller's barrier handling: a first send is journaled (`record`)
/// and its outbox released, a re-send is acked from the existing entry —
/// unless this operative's ack is lost on the way back.
pub(crate) fn journal_barrier(
    operative_id: &str,
    reference_id: &str,
    flushed: Vec<Vec<u8>>,
    record: impl FnOnce(),
) -> Vec<Vec<u8>> {
    BARRIER_SENDS
        .lock()
        .unwrap()
        .push((operative_id.to_string(), reference_id.to_string()));
    let fresh = {
        let mut refs = JOURNALED_REFS.lock().unwrap();
        let fresh = !refs.iter().any(|r| r == reference_id);
        if fresh {
            refs.push(reference_id.to_string());
        }
        fresh
    };
    if fresh {
        record();
    }
    if !ack_lost(operative_id, fresh) {
        crate::shutdown::signal_result(reference_id, Ok(String::new()));
    }
    if fresh {
        flushed
    } else {
        Vec::new()
    }
}

pub(crate) fn wire_emits_for(id: &str) -> Vec<Vec<u8>> {
    WIRE_EMITS
        .lock()
        .unwrap()
        .iter()
        .filter(|(oid, _)| oid == id)
        .map(|(_, p)| p.clone())
        .collect()
}

/// Fake-controller half of every SDK-initiated control request: record the
/// frame and, for the two durability handshakes, answer the barrier with an
/// ack success. Returns the outbox the (real) controller would flush
/// to the plugins AFTER that durable point — the drain helpers append it to
/// the observed output, so a staged operative's assertions still describe
/// what a plugin finally sees. A park is one-way: nothing to ack.
pub(crate) fn ack_control_request(
    reference_id: String,
    command: crate::pb::command_request::Command,
) -> Vec<Vec<u8>> {
    use crate::pb::command_request::Command;
    match command {
        Command::TaskCommit(commit) => {
            let flushed: Vec<Vec<u8>> = commit.outbox.iter().map(|o| o.payload.clone()).collect();
            let operative_id = commit.operative_id.clone();
            journal_barrier(&operative_id, &reference_id, flushed, || {
                TASK_COMMITS.lock().unwrap().push(commit)
            })
        }
        Command::IterCommit(commit) => {
            let operative_id = commit.operative_id.clone();
            journal_barrier(&operative_id, &reference_id, Vec::new(), || {
                ITER_COMMITS.lock().unwrap().push(commit)
            })
        }
        Command::IterPark(park) => {
            ITER_PARKS.lock().unwrap().push(park);
            Vec::new()
        }
        _ => Vec::new(),
    }
}

/// The operative id an SDK-initiated control request belongs to.
pub(crate) fn control_request_operative(command: &crate::pb::command_request::Command) -> String {
    use crate::pb::command_request::Command;
    match command {
        Command::TaskCommit(c) => c.operative_id.clone(),
        Command::IterCommit(c) => c.operative_id.clone(),
        Command::IterPark(p) => p.operative_id.clone(),
        _ => String::new(),
    }
}

/// Bind workflow `id` of `factory` as `AddOperative` does, resuming with
/// `expected` Cell restores, `owed_inbox` re-forwarded messages and the
/// journal; `Ok(false)` names an unregistered class, `Err` a refused
/// `init_values`.
pub(crate) fn start_resumable(
    node: &RunnerNode,
    factory: String,
    id: String,
    init_values: &str,
    expected: u32,
    owed_inbox: u32,
    journal: Option<flz_core::workflow::instance::JournalResume>,
) -> Result<bool, flz_core::Error> {
    // A duplicate never initializes twice, as `AddOperative` guards it.
    if node.has_operative(&id) {
        return Ok(true);
    }
    let resume = flz_core::workflow::Resume {
        expected: expected as usize,
        owed_inbox,
        journal,
    };
    let Some(instance) = node.start_operative(
        factory,
        id.clone(),
        flz_core::operative::instance::KeyGroupRange::UNPARTITIONED,
        resume,
    ) else {
        return Ok(false);
    };
    if let Err(e) = flz_core::command::cell_state::apply_init_values(&instance, init_values) {
        node.stop_operative(&id);
        return Err(e);
    }
    flz_core::workflow::open(&instance);
    Ok(true)
}

/// A fresh bind of workflow `id`.
pub(crate) fn start(
    node: &RunnerNode,
    factory: String,
    id: String,
    init_values: &str,
) -> Result<bool, flz_core::Error> {
    start_resumable(node, factory, id, init_values, 0, 0, None)
}

pub(crate) fn instance(
    node: &RunnerNode,
    id: &str,
) -> Option<Arc<flz_core::operative::instance::OperativeInstance>> {
    node.get_operative(id)
}

/// A live Cell by wire identity, with its instance: where a restore lands.
pub(crate) fn find_cell(
    node: &RunnerNode,
    cell_id: &[u8],
) -> Option<(
    Arc<flz_core::operative::instance::OperativeInstance>,
    Arc<crate::cell::CellSlot>,
)> {
    node.find_cell(cell_id)
}

/// One Cell restore landed, carrying no saga marker.
pub(crate) fn restored(instance: &flz_core::operative::instance::OperativeInstance) {
    flz_core::workflow::restore_landed(instance, &[], false, false);
}

/// One message into workflow `id` through the entrance, on its first entry
/// channel; `false` when no such instance is registered.
pub(crate) fn feed(node: &RunnerNode, id: &str, message: &[u8]) -> bool {
    feed_on(node, id, None, message)
}

/// One message into workflow `id` naming `channel`, as `flz operative send
/// --channel` does.
pub(crate) fn feed_on(node: &RunnerNode, id: &str, channel: Option<&str>, message: &[u8]) -> bool {
    let metadata = channel
        .map(|name| std::collections::HashMap::from([("channel".to_string(), name.to_string())]))
        .unwrap_or_default();
    dispatch(node, id, message, &metadata)
}

/// One message into `instance` stamped with the input-log offset the
/// controller would have given it.
pub(crate) fn feed_at(
    instance: &Arc<flz_core::operative::instance::OperativeInstance>,
    message: &[u8],
    offset: u64,
) {
    let registry = Arc::new(flz_core::operative::registry::OperativeRegistry::new());
    registry.register(Arc::clone(instance));
    let metadata =
        std::collections::HashMap::from([("flz.offset".to_string(), offset.to_string())]);
    flz_core::runner::OperativeDispatchHandle { registry }.dispatch(
        &instance.instance_id,
        message.to_vec(),
        &metadata,
        flz_core::workflow::unwired_stream(),
    );
}

/// A frame through the dispatch a stream reader makes.
fn dispatch(
    node: &RunnerNode,
    id: &str,
    message: &[u8],
    metadata: &std::collections::HashMap<String, String>,
) -> bool {
    if !node.has_operative(id) {
        return false;
    }
    node.operative_dispatch_handle().dispatch(
        id,
        message.to_vec(),
        metadata,
        flz_core::workflow::unwired_stream(),
    );
    true
}

/// Start `factory` under `id` and hand it `message` as its first input: what
/// a spawn, a `send` or `flz operative start --text` does.
pub(crate) fn start_fed(node: &RunnerNode, factory: &str, id: &str, message: &[u8]) -> bool {
    start(node, factory.into(), id.into(), "").expect("init_values is a JSON object")
        && feed(node, id, message)
}

/// Drain output frames until the `WorkflowCompleted` for `id`, returning the
/// collected outputs and the `(status, error, result)` completion tuple.
/// "Output" is what reaches the plugins: an opted-out emit straight off the
/// wire, plus a durable operative's staged outbox once its task commits (the
/// fake controller flushes it on ack).
pub(crate) fn drain(
    rx: &Receiver<RunnerMessage>,
    id: &str,
) -> (Vec<Vec<u8>>, (String, String, Vec<u8>)) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut outs = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let msg = rx
            .recv_timeout(remaining)
            .unwrap_or_else(|_| panic!("timed out waiting for workflow {id}"));
        match msg {
            RunnerMessage::EmitWorkflowOutput {
                operative_id,
                payload,
            } if operative_id == id => {
                WIRE_EMITS
                    .lock()
                    .unwrap()
                    .push((operative_id, payload.clone()));
                outs.push(payload);
            }
            RunnerMessage::EmitWorkflowCompleted {
                operative_id,
                status,
                error,
                result,
            } if operative_id == id => return (outs, (status, error, result)),
            RunnerMessage::EmitControlRequest {
                reference_id,
                command,
            } => {
                let for_id = control_request_operative(&command) == id;
                // A park tears the run down WITHOUT a completion, so the
                // drain would otherwise wait out its deadline: report it as
                // the terminal outcome of this run instead.
                let parked = matches!(*command, crate::pb::command_request::Command::IterPark(_));
                let flushed = ack_control_request(reference_id, *command);
                if for_id {
                    outs.extend(flushed);
                    if parked {
                        return (outs, ("parked".to_string(), String::new(), Vec::new()));
                    }
                }
            }
            _ => {}
        }
    }
}

/// A completion for `id` within `wait` fails the scenario; control requests
/// seen meanwhile are acked as the controller would.
pub(crate) fn assert_no_completion(rx: &Receiver<RunnerMessage>, id: &str, wait: Duration) {
    let deadline = Instant::now() + wait;
    while let Ok(message) = rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        match message {
            RunnerMessage::EmitWorkflowCompleted {
                operative_id,
                status,
                ..
            } => assert!(
                operative_id != id,
                "workflow {id} ended with status `{status}` instead of staying quiet"
            ),
            RunnerMessage::EmitControlRequest {
                reference_id,
                command,
            } => {
                let _ = ack_control_request(reference_id, *command);
            }
            _ => {}
        }
    }
}

/// Block until a specific output frame for `id` is observed, confirming an
/// interactive instance is live before stopping it.
pub(crate) fn wait_for_output(rx: &Receiver<RunnerMessage>, id: &str, payload: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let msg = rx
            .recv_timeout(remaining)
            .unwrap_or_else(|_| panic!("timed out waiting for output from {id}"));
        match msg {
            RunnerMessage::EmitControlRequest {
                reference_id,
                command,
            } => {
                let for_id = control_request_operative(&command) == id;
                let flushed = ack_control_request(reference_id, *command);
                if for_id && flushed.iter().any(|p| p == payload) {
                    return;
                }
            }
            RunnerMessage::EmitWorkflowOutput {
                operative_id,
                payload: p,
            } => {
                if operative_id == id && p == payload {
                    return;
                }
            }
            _ => {}
        }
    }
}
