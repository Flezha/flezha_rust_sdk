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
/// controller's: the entry is journaled and the records it names released,
/// only the reply never reaches the runner.
pub(crate) static LOST_ACKS: Mutex<Vec<(String, usize)>> = Mutex::new(Vec::new());

/// Every plugin frame written STRAIGHT to the wire, bypassing any task
/// boundary. A durable structured operative writes none (its output rides its
/// Cells' effect records); an opted-out class does.
pub(crate) static WIRE_EMITS: Mutex<Vec<(String, Vec<u8>)>> = Mutex::new(Vec::new());

/// What a plugin receives, in order, per operative: an opted-out class's wire
/// output, and a durable one's record outputs once the commit naming the
/// records is acked.
pub(crate) static DELIVERED: Mutex<Vec<(String, Vec<u8>)>> = Mutex::new(Vec::new());

/// The wire a run's plugin frames are written to: each parsed back and
/// recorded as delivered.
pub(crate) fn capturing_wire() -> Arc<flz_transport::transport::FlezhaStream> {
    Arc::new(flz_transport::transport::FlezhaStream::Local(Arc::new(
        |frame: &[u8]| {
            let (operative_id, payload, _metadata) =
                flz_transport::service::payloader::extract_payload(frame)?;
            WIRE_EMITS
                .lock()
                .unwrap()
                .push((operative_id.clone(), payload.clone()));
            DELIVERED.lock().unwrap().push((operative_id, payload));
            Ok(())
        },
    )))
}

/// `id`'s deliveries since the last take.
pub(crate) fn take_delivered(id: &str) -> Vec<Vec<u8>> {
    let mut delivered = DELIVERED.lock().unwrap();
    let (mine, rest): (Vec<_>, Vec<_>) = delivered.drain(..).partition(|(oid, _)| oid == id);
    *delivered = rest;
    mine.into_iter().map(|(_, payload)| payload).collect()
}

/// The outputs of the effect records `cells` name, as the controller releases
/// them once the entry naming them is durable, read back from the frames the
/// run shipped to the state sink.
pub(crate) fn released(cells: &[crate::pb::CommittedCell]) -> Vec<Vec<u8>> {
    let frames = super::fixtures::WAL_FRAMES.lock().unwrap();
    let records: Vec<(Vec<u8>, u64, Vec<Vec<u8>>)> = frames
        .iter()
        .filter(|(pid, _)| pid == crate::cell::state_sink::CELL_EFFECT_RECORD_PAYLOAD_ID)
        .filter_map(|(_, frame)| effect_record(frame))
        .collect();
    cells
        .iter()
        .filter_map(|cell| Some((cell, cell.first_seq?, cell.seq?)))
        .flat_map(|(cell, first, last)| {
            records
                .iter()
                .filter(|(id, seq, _)| *id == cell.cell_id && (first..=last).contains(seq))
                .flat_map(|(_, _, outputs)| outputs.clone())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// A `cell_effect_record` frame's Cell, seq and output payloads.
fn effect_record(frame: &[u8]) -> Option<(Vec<u8>, u64, Vec<Vec<u8>>)> {
    let split = crate::cell::flusher::split_cell_frame(frame)?;
    let record: crate::cell::flusher::EffectRecordBody = postcard::from_bytes(split.body).ok()?;
    Some((
        split.identity.to_vec(),
        split.seq,
        record.outputs.into_iter().map(|o| o.payload).collect(),
    ))
}

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
/// and the records it names released, a re-send is acked from the existing
/// entry, unless this operative's ack is lost on the way back.
pub(crate) fn journal_barrier(
    operative_id: &str,
    reference_id: &str,
    released: Vec<Vec<u8>>,
    record: impl FnOnce(),
) {
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
        let mut delivered = DELIVERED.lock().unwrap();
        delivered.extend(released.into_iter().map(|p| (operative_id.to_string(), p)));
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
/// ack success, releasing what a task commit names to `DELIVERED` as the real
/// controller releases it AFTER that durable point. A park is one-way:
/// nothing to ack.
pub(crate) fn ack_control_request(
    reference_id: String,
    command: crate::pb::command_request::Command,
) {
    use crate::pb::command_request::Command;
    match command {
        Command::TaskCommit(commit) => {
            let operative_id = commit.operative_id.clone();
            journal_barrier(
                &operative_id,
                &reference_id,
                released(&commit.cells_ran),
                || TASK_COMMITS.lock().unwrap().push(commit),
            )
        }
        Command::IterCommit(commit) => {
            let operative_id = commit.operative_id.clone();
            journal_barrier(&operative_id, &reference_id, Vec::new(), || {
                ITER_COMMITS.lock().unwrap().push(commit)
            })
        }
        Command::IterPark(park) => ITER_PARKS.lock().unwrap().push(park),
        _ => {}
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
    let resume = flz_core::workflow::Resume {
        expected: expected as usize,
        owed_inbox,
        journal,
    };
    bind(
        node,
        factory,
        id,
        init_values,
        flz_core::operative::instance::KeyGroupRange::UNPARTITIONED,
        resume,
    )
}

/// A fresh bind of workflow `id` as the partition member owning `range`.
pub(crate) fn start_partitioned(
    node: &RunnerNode,
    factory: &str,
    id: &str,
    range: flz_core::operative::instance::KeyGroupRange,
) -> bool {
    bind(
        node,
        factory.into(),
        id.into(),
        "",
        range,
        flz_core::workflow::Resume::default(),
    )
    .expect("init_values is a JSON object")
}

fn bind(
    node: &RunnerNode,
    factory: String,
    id: String,
    init_values: &str,
    range: flz_core::operative::instance::KeyGroupRange,
    resume: flz_core::workflow::Resume,
) -> Result<bool, flz_core::Error> {
    // A duplicate never initializes twice, as `AddOperative` guards it.
    if node.has_operative(&id) {
        return Ok(true);
    }
    let Some(instance) = node.start_operative(factory, id.clone(), range, resume) else {
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

/// Drain control requests until the `WorkflowCompleted` for `id`, returning
/// what reached its plugins and the `(status, error, result)` completion
/// tuple: an opted-out emit straight off the wire, a durable operative's
/// record outputs once the commit naming them is acked.
pub(crate) fn drain(
    rx: &Receiver<RunnerMessage>,
    id: &str,
) -> (Vec<Vec<u8>>, (String, String, Vec<u8>)) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let msg = rx
            .recv_timeout(remaining)
            .unwrap_or_else(|_| panic!("timed out waiting for workflow {id}"));
        match msg {
            RunnerMessage::EmitWorkflowCompleted {
                operative_id,
                status,
                error,
                result,
            } if operative_id == id => return (take_delivered(id), (status, error, result)),
            RunnerMessage::EmitControlRequest {
                reference_id,
                command,
            } => {
                let for_id = control_request_operative(&command) == id;
                // A park tears the run down WITHOUT a completion, so the
                // drain would otherwise wait out its deadline: report it as
                // the terminal outcome of this run instead.
                let parked = matches!(*command, crate::pb::command_request::Command::IterPark(_));
                ack_control_request(reference_id, *command);
                if for_id && parked {
                    return (
                        take_delivered(id),
                        ("parked".to_string(), String::new(), Vec::new()),
                    );
                }
            }
            _ => {}
        }
    }
}

/// What reached `id`'s plugins until `done` holds, every control request
/// answered on the way.
pub(crate) fn pump(
    rx: &Receiver<RunnerMessage>,
    id: &str,
    done: impl Fn(&[Vec<u8>]) -> bool,
) -> Vec<Vec<u8>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut outs = Vec::new();
    loop {
        outs.extend(take_delivered(id));
        if done(&outs) {
            return outs;
        }
        assert!(
            Instant::now() < deadline,
            "{id} delivered {outs:?} and then nothing"
        );
        if let Ok(RunnerMessage::EmitControlRequest {
            reference_id,
            command,
        }) = rx.recv_timeout(Duration::from_millis(20))
        {
            ack_control_request(reference_id, *command);
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
            } => ack_control_request(reference_id, *command),
            _ => {}
        }
    }
}

/// Block until a specific output of `id` reaches its plugins, confirming an
/// interactive instance is live before stopping it.
pub(crate) fn wait_for_output(rx: &Receiver<RunnerMessage>, id: &str, payload: &[u8]) {
    pump(rx, id, |outs| outs.iter().any(|p| p == payload));
}
