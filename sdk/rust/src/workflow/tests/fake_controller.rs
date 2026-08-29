//! The controller half the driver runs against: what a barrier send, a
//! journal entry and a lost ack look like from the runner's side, plus the
//! drains that turn `RunnerMessage`s into a scenario's observations.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use crossbeam::channel::Receiver;

use crate::runner::RunnerMessage;

/// Proc-commits observed by the fake controller (`workflow-proc-journal`
/// barrier): every `ProcCommit` the drain helpers ack is recorded here
/// so the gating/handoff assertions can inspect what would be journaled.
pub(crate) static PROC_COMMITS: Mutex<Vec<crate::pb::ProcCommit>> = Mutex::new(Vec::new());

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
/// reply never reaches the driver.
pub(crate) static LOST_ACKS: Mutex<Vec<(String, usize)>> = Mutex::new(Vec::new());

/// Every `EmitWorkflowOutput` the drain helpers saw — output that went
/// STRAIGHT to the wire, bypassing any proc boundary. A durable structured
/// operative must produce none of these (it stages); `run`-sugar and
/// opted-out classes still do.
pub(crate) static WIRE_EMITS: Mutex<Vec<(String, Vec<u8>)>> = Mutex::new(Vec::new());

pub(crate) fn drain_proc_commits() -> Vec<crate::pb::ProcCommit> {
    std::mem::take(&mut *PROC_COMMITS.lock().unwrap())
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
/// frame and, for the two durability handshakes, wake the blocked driver
/// with an ack success. Returns the outbox the (real) controller would flush
/// to the plugins AFTER that durable point — the drain helpers append it to
/// the observed output, so a staged operative's assertions still describe
/// what a plugin finally sees. A park is one-way: nothing to ack.
pub(crate) fn ack_control_request(
    reference_id: String,
    command: crate::pb::command_request::Command,
) -> Vec<Vec<u8>> {
    use crate::pb::command_request::Command;
    match command {
        Command::ProcCommit(commit) => {
            let flushed: Vec<Vec<u8>> = commit.outbox.iter().map(|o| o.payload.clone()).collect();
            let operative_id = commit.operative_id.clone();
            journal_barrier(&operative_id, &reference_id, flushed, || {
                PROC_COMMITS.lock().unwrap().push(commit)
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
        Command::ProcCommit(c) => c.operative_id.clone(),
        Command::IterCommit(c) => c.operative_id.clone(),
        Command::IterPark(p) => p.operative_id.clone(),
        _ => String::new(),
    }
}

/// Drain output frames until the `WorkflowCompleted` for `id`, returning the
/// collected outputs and the `(status, error, result)` completion tuple.
/// "Output" is what reaches the plugins: a `run`-sugar / opted-out emit
/// straight off the wire, plus a structured operative's staged outbox once
/// its proc commits (the fake controller flushes it on ack).
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
                let parked = matches!(command, crate::pb::command_request::Command::IterPark(_));
                let flushed = ack_control_request(reference_id, command);
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
                let flushed = ack_control_request(reference_id, command);
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
