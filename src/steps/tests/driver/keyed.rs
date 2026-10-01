//! Keyed steps tasklets: Cells minted per key by routing, recorded and
//! undone by identity, restored on a resume, capped as a flow operator's, and swept
//! only between passes and never within a rollback's reach.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crossbeam::channel::Receiver;
use flz_core::command::CommandAction;
use flz_core::dynamic::DynamicCellRegistry;
use flz_core::runner::RunnerNode;
use prost::Message;

use super::super::fake_controller::*;
use super::super::fixtures::*;
use crate::cell::flusher::encode_cell_identity;
use crate::runner::RunnerMessage;

fn registry(node: &RunnerNode, id: &str, tasklet: &str) -> Arc<DynamicCellRegistry> {
    let inst = instance(node, id).expect("a live instance");
    let tasklet_id = inst.topology.tasklet_name_to_id[tasklet] as usize;
    Arc::clone(
        inst.dynamic_registries[tasklet_id]
            .as_ref()
            .expect("a keyed tasklet has a registry"),
    )
}

fn live_keys(registry: &DynamicCellRegistry) -> Vec<String> {
    let mut keys: Vec<String> = registry.cells.iter().map(|e| e.key().clone()).collect();
    keys.sort();
    keys
}

/// Every Cell of `registry` idle past a zero TTL, swept once.
fn sweep(registry: &DynamicCellRegistry) {
    std::thread::sleep(Duration::from_millis(5));
    registry.reap_sweep(Duration::ZERO);
}

fn cells_ran_of(commits: &[crate::pb::TaskCommit], task_idx: u32) -> Vec<Vec<u8>> {
    commits
        .iter()
        .find(|c| c.task_idx == task_idx && !c.compensating)
        .map(|c| {
            c.cells_ran
                .iter()
                .map(|cell| cell.cell_id.clone())
                .collect()
        })
        .expect("the stage committed")
}

/// A restore frame through the command handler, as the controller ships it.
fn restore(
    node: &mut RunnerNode,
    cell_id: Vec<u8>,
    terminal_seq: u64,
    intent: bool,
    done: bool,
) -> crate::pb::RestoreInitStateResponse {
    let command = crate::pb::RestoreInitState {
        cell_id,
        terminal_seq,
        has_compensation_intent: intent,
        has_compensation_done: done,
        ..Default::default()
    };
    let mut errors = Vec::new();
    let reply = command.handle(node, "", &mut errors);
    assert!(errors.is_empty(), "the restore applied: {errors:?}");
    crate::pb::RestoreInitStateResponse::decode(reply.bytes.as_slice())
        .expect("a restore answers a response")
}

pub(super) fn keyed_cells(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- one Cell per key, minted as the stage's items are addressed; the
    // commit names each minted Cell once, whatever the item count ---
    let _ = drain_task_commits();
    let id = "keyed-flow";
    assert!(start_fed(node, "test-keyed-flow", id, b"a,b,a,c"));
    let (_o, done) = drain(rx, id);
    assert_eq!(done.0, "ok");
    assert_eq!(
        live_keys(&registry(node, id, "keyed_undo")),
        ["a", "b", "c"]
    );
    assert_eq!(
        cells_ran_of(&drain_task_commits(), 1),
        ["a", "b", "c"].map(|k| encode_cell_identity(id, "keyed_undo", k)),
        "the keyed stage's commit names the three minted Cells, once each"
    );

    // --- a later failure undoes once per key ---
    let _ = drain_comp_log();
    let id = "keyed-saga";
    assert!(start_fed(node, "test-keyed-saga", id, b"a,b,c"));
    let (_o, done) = drain(rx, id);
    assert_eq!(done.0, "error");
    let mut undone = drain_comp_log();
    undone.sort();
    assert_eq!(undone, ["undo-keyed:a", "undo-keyed:b", "undo-keyed:c"]);
    let _ = drain_task_commits();

    // --- the cap drops a new key, counted; the pass runs without it ---
    let id = "keyed-capped";
    assert!(start_fed(node, "test-capped-keyed", id, b"a,b,c"));
    let (_o, done) = drain(rx, id);
    assert_eq!(done.0, "ok", "the capped item is not owed");
    let capped = registry(node, id, "capped_keyed");
    assert_eq!(live_keys(&capped), ["a", "b"]);
    assert_eq!(
        capped
            .counters
            .inputs_dropped_capped_total
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(cells_ran_of(&drain_task_commits(), 1).len(), 2);

    // --- a static stage's commit names its Cells in declared order, as it
    // always has ---
    let id = "static-order";
    assert!(start_fed(node, "test-static-order", id, b"x"));
    let (_o, done) = drain(rx, id);
    assert_eq!(done.0, "ok");
    assert_eq!(
        cells_ran_of(&drain_task_commits(), 0),
        ["z", "y"].map(|k| encode_cell_identity(id, "zy_static", k))
    );

    journaled_keyed_cells(node, rx);
    restored_keyed_cells(node, rx);
    keyed_sweep(node, rx);
    partition_member(node, rx);
}

/// A resume reads a journaled `cells_ran` naming keyed Cells back onto the
/// instance, and a restore materialises the keyed Cell it names.
fn journaled_keyed_cells(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    let _ = drain_comp_log();
    let id = "keyed-journal";
    let topology = flz_core::operator::registry::get_class("test-keyed-saga").expect("registered");
    let keyed = topology.tasklet_name_to_id["keyed_undo"];
    let handoff_channel = topology.tasklets[keyed as usize]
        .output_channel_id
        .expect("the keyed stage releases on a channel");
    let split = encode_cell_identity(id, "key_split", "");
    let journal = flz_core::steps::instance::JournalResume {
        committed_task_idxs: vec![0, 1],
        task_handoffs: vec![flz_core::steps::instance::EdgeHandoff {
            task_idx: 1,
            handoff: vec![(
                handoff_channel,
                postcard::to_allocvec(&"a:x".to_string()).unwrap(),
            )],
        }],
        committed_tasks: vec![
            flz_core::steps::instance::CommittedTask {
                task_idx: 0,
                iter: 0,
                cells_ran: vec![split],
                completion: None,
                trigger: None,
            },
            flz_core::steps::instance::CommittedTask {
                task_idx: 1,
                iter: 0,
                cells_ran: ["a", "b"]
                    .map(|k| encode_cell_identity(id, "keyed_undo", k))
                    .into(),
                completion: None,
                trigger: None,
            },
        ],
        ..Default::default()
    };
    assert!(start_resumable(
        node,
        "test-keyed-saga".into(),
        id.into(),
        "",
        0,
        0,
        Some(journal)
    )
    .expect("init_values is a JSON object"));
    assert!(feed(node, id, b"a,b"));
    let (outs, done) = drain(rx, id);
    assert_eq!(done.0, "error", "the resumed last stage fails");
    assert!(outs.is_empty());
    let mut undone = drain_comp_log();
    undone.sort();
    assert_eq!(
        undone,
        ["undo-keyed:a", "undo-keyed:b"],
        "each journaled keyed Cell is found by identity, minted, and undone once"
    );
    let _ = drain_task_commits();
}

fn restored_keyed_cells(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- a resumed rollback: the restores mint the keyed Cells they name and
    // carry the markers; the one already undone is skipped ---
    let _ = drain_comp_log();
    let id = "keyed-intents";
    assert!(
        start_resumable(node, "test-keyed-saga".into(), id.into(), "", 2, 0, None)
            .expect("init_values is a JSON object")
    );
    assert!(feed(node, id, b"a,b"));
    let a = encode_cell_identity(id, "keyed_undo", "a");
    let b = encode_cell_identity(id, "keyed_undo", "b");
    restore(node, a, 4, true, false);
    assert_eq!(
        registry(node, id, "keyed_undo")
            .cells
            .get("a")
            .expect("the restore minted the Cell")
            .change_seq
            .load(Ordering::Acquire),
        5,
        "the restore folded into the minted Cell and re-armed its seq"
    );
    restore(node, b, 4, true, true);
    let (_o, done) = drain(rx, id);
    assert_eq!(done.0, "error");
    assert_eq!(drain_comp_log(), ["undo-keyed:a"]);
    let _ = drain_task_commits();

    // --- a restore the cap refuses acks its terminal seq and still counts,
    // so the gate opens on the frames that landed and the pass runs ---
    let id = "keyed-capped-restore";
    assert!(
        start_resumable(node, "test-capped-keyed".into(), id.into(), "", 3, 0, None)
            .expect("init_values is a JSON object")
    );
    for key in ["a", "b"] {
        restore(
            node,
            encode_cell_identity(id, "capped_keyed", key),
            7,
            false,
            false,
        );
    }
    let refused = restore(
        node,
        encode_cell_identity(id, "capped_keyed", "c"),
        7,
        false,
        false,
    );
    assert_eq!(
        refused.terminal_seq, 7,
        "the refused frame acks its terminal seq"
    );
    assert_eq!(live_keys(&registry(node, id, "capped_keyed")), ["a", "b"]);
    assert!(feed(node, id, b"a"));
    let (_o, done) = drain(rx, id);
    assert_eq!(
        done.0, "ok",
        "the gate opened with the refused frame counted"
    );
    let _ = drain_task_commits();
}

fn keyed_sweep(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- nothing is swept while a pass runs, however idle ---
    KEYED_BLOCK_RELEASE.store(false, Ordering::SeqCst);
    let id = "keyed-block";
    assert!(start_fed(node, "test-keyed-block", id, b"a,b"));
    let keyed = registry(node, id, "keyed_undo");
    while live_keys(&keyed).len() < 2 {
        std::thread::yield_now();
    }
    sweep(&keyed);
    assert_eq!(
        live_keys(&keyed),
        ["a", "b"],
        "a running pass keeps its Cells"
    );
    KEYED_BLOCK_RELEASE.store(true, Ordering::SeqCst);
    let (_o, done) = drain(rx, id);
    assert_eq!(done.0, "ok");

    // --- between passes: all-or-nothing keeps what its rollback could still
    // undo and frees a hookless Cell; keep-progress frees both. The loops
    // wait for their next message rather than parking meanwhile. ---
    for (factory, id, kept) in [
        ("test-keyed-loop-aon", "keyed-loop-aon", vec!["a", "b"]),
        ("test-keyed-loop-kp", "keyed-loop-kp", vec![]),
    ] {
        assert!(start_fed(node, factory, id, b"a,b"));
        assert_no_completion(rx, id, Duration::from_millis(300));
        let undo = registry(node, id, "keyed_undo");
        let plain = registry(node, id, "keyed_plain");
        assert_eq!(live_keys(&plain), ["a:a", "b:b"]);
        sweep(&undo);
        sweep(&plain);
        assert_eq!(live_keys(&undo), kept, "{factory}: the compensable stage");
        assert!(
            live_keys(&plain).is_empty(),
            "{factory}: the hookless stage"
        );
        flz_core::steps::stop(&instance(node, id).expect("live"));
        let (_o, done) = drain(rx, id);
        assert_eq!(done.0, "cancelled");
    }
    let _ = drain_task_commits();
    let _ = drain_iter_commits();
}

/// The parks shipped for `id` since `WAL_FRAMES` was cleared, once each,
/// acked so their parking threads return.
fn acked_parks(id: &str) -> Vec<crate::cell::park::ParkBody> {
    let frames: Vec<Vec<u8>> = WAL_FRAMES
        .lock()
        .unwrap()
        .iter()
        .filter(|(payload_id, _)| payload_id == crate::cell::state_sink::PARK_PAYLOAD_PAYLOAD_ID)
        .map(|(_, frame)| frame.clone())
        .collect();
    let mut parks: Vec<crate::cell::park::ParkBody> = Vec::new();
    for frame in frames {
        // `[id_len(2)][identity][seq(8)][header(16)][body_len(4)][body][crc(4)]`
        let id_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
        let rest = &frame[2 + id_len + 8 + 16..];
        let len = u32::from_be_bytes(rest[..4].try_into().unwrap()) as usize;
        let body: crate::cell::park::ParkBody =
            postcard::from_bytes(&rest[4..4 + len]).expect("a postcard ParkBody");
        if body.park_id.starts_with(&format!("{id}/"))
            && !parks.iter().any(|p| p.park_id == body.park_id)
        {
            crate::cell::park::complete_ack(&body.park_id);
            parks.push(body);
        }
    }
    parks
}

/// Answer `id`'s control requests until its next iteration commits.
fn await_iter_commit(rx: &Receiver<RunnerMessage>, id: &str) -> crate::pb::IterCommit {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(commit) = drain_iter_commits()
            .into_iter()
            .find(|c| c.operator_id == id)
        {
            return commit;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{id} committed no iteration"
        );
        if let Ok(RunnerMessage::EmitControlRequest {
            reference_id,
            command,
        }) = rx.recv_timeout(Duration::from_millis(10))
        {
            ack_control_request(reference_id, *command);
        }
    }
}

fn partition_member(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- a partition member parks a key it does not own under the entry
    // channel before any Cell exists, opens no pass, and runs its next
    // in-range message as its first iteration. "a" hashes to key group 100
    // and "BTC-USD" to 129 ---
    let id = "keyed-entry-p1";
    let range = flz_core::operator::instance::KeyGroupRange {
        index: 1,
        count: 2,
        from: 128,
        to: 255,
    };
    let _ = drain_task_commits();
    let _ = drain_iter_commits();
    WAL_FRAMES.lock().unwrap().clear();
    assert!(start_partitioned(node, "test-keyed-entry-loop", id, range));
    let inst = instance(node, id).expect("live");

    feed_at(&inst, b"a", 3);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let parks = loop {
        let parks = acked_parks(id);
        if !parks.is_empty() || std::time::Instant::now() >= deadline {
            break parks;
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(parks.len(), 1, "the mis-routed message parks once");
    assert_eq!(parks[0].entry.channel, "in");
    assert_eq!(parks[0].origin_offset, Some(3));
    assert!(
        parks[0]
            .entry
            .error
            .contains("key 'a' hashes to key group 100"),
        "{}",
        parks[0].entry.error
    );
    assert!(live_keys(&registry(node, id, "keyed_plain")).is_empty());
    assert!(drain_task_commits().is_empty(), "no stage ran");

    feed_at(&inst, b"BTC-USD", 4);
    let commit = await_iter_commit(rx, id);
    assert_eq!(
        (commit.iter, commit.consumed_offset),
        (0, Some(4)),
        "the in-range message is the first iteration"
    );
    assert_eq!(live_keys(&registry(node, id, "keyed_plain")), ["BTC-USD"]);

    flz_core::steps::stop(&inst);
    let (_o, done) = drain(rx, id);
    assert_eq!(done.0, "cancelled");
    let _ = drain_task_commits();
}
