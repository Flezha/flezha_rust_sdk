//! Fan-in, merge and branching: the join that runs once after both
//! producers, the mapped handoff, and resume over a branch.

use crossbeam::channel::Receiver;
use flz_core::runner::RunnerNode;
use flz_core::test_support::steps::*;

use super::super::fixtures::*;
use crate::cell::state_sink::COMPENSATION_DONE_PAYLOAD_ID as DONE;
use crate::runner::RunnerMessage;
use crate::steps::*;

/// The journal a diamond-shaped chain leaves when A (the fan) and B (one
/// branch) committed before the crash: each producer's edge handoff, plus
/// the cells each of them ran. Shared by the branching resume and by both
/// merge resumes — the shape is the same, only the channels and the cells
/// each task ran differ.
fn ab_committed(
    fan: u32,
    b: u32,
    a_segs: Vec<Vec<u8>>,
    b_segs: Vec<Vec<u8>>,
) -> instance::JournalResume {
    let enc = |s: &str| postcard::to_allocvec(&s.to_string()).unwrap();
    instance::JournalResume {
        committed_task_idxs: vec![0, 1],
        task_handoffs: vec![
            instance::EdgeHandoff {
                task_idx: 0,
                handoff: vec![(fan, enc("a:x"))],
            },
            instance::EdgeHandoff {
                task_idx: 1,
                handoff: vec![(b, enc("b:a:x"))],
            },
        ],
        committed_tasks: vec![
            instance::CommittedTask {
                task_idx: 0,
                iter: 0,
                cells_ran: a_segs,
                completion: None,
                trigger: None,
            },
            instance::CommittedTask {
                task_idx: 1,
                iter: 0,
                cells_ran: b_segs,
                completion: None,
                trigger: None,
            },
        ],
        ..Default::default()
    }
}

pub(super) fn fan_in_branching(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- steps-branching fan-in: the join runs once, after BOTH
    // producers committed, with the accumulated batch ---
    let _ = drain_diamond_log();
    let _ = drain_task_commits();
    assert!(start_fed(node, "test-diamond-steps", "diamond-1", b"x"));
    let (outs, done) = drain(rx, "diamond-1");
    assert_eq!(done.0, "ok");
    assert_eq!(
        outs,
        vec![b"j:b:a:x".to_vec(), b"j:c:a:x".to_vec()],
        "the join's input batch accumulates BOTH producers' outputs (B's first: sibling order)"
    );
    // The two producers commit on their own, in whichever order they finish.
    let mut commits: Vec<u32> = drain_task_commits().iter().map(|c| c.task_idx).collect();
    commits[1..3].sort_unstable();
    assert_eq!(
        commits,
        vec![0, 1, 2, 3],
        "one commit per node — the join commits exactly once, after both producers"
    );
    let log = drain_diamond_log();
    let first_join = log
        .iter()
        .position(|e| e.starts_with("j:"))
        .expect("the join ran");
    assert!(
        log[..first_join].iter().any(|e| e.starts_with("b:"))
            && log[..first_join].iter().any(|e| e.starts_with("c:")),
        "both producers ran before the join was dispatched: {log:?}"
    );
    assert_eq!(
        log.iter().filter(|e| e.starts_with("j:")).count(),
        2,
        "the join processed each contribution exactly once"
    );

    // --- steps-branching crash-resume over a join: one journaled and one
    // fresh producer — the committed branch never re-runs, and the join
    // accumulates B's JOURNALED handoff with C's fresh output ---
    let _ = drain_diamond_log();
    let da_id = crate::cell::flusher::encode_cell_identity("diamond-r", "diamond_a", "");
    let diamond_spec =
        crate::relay::registry::get_class("test-diamond-steps").expect("diamond class registered");
    let diamond_topo = &diamond_spec;
    let d_fan = diamond_topo.lookup_channel_id("d_fan").unwrap();
    let d_b = diamond_topo.lookup_channel_id("d_b").unwrap();
    assert!(start_resumable(
        node,
        "test-diamond-steps".into(),
        "diamond-r".into(),
        "",
        1,
        0,
        Some(ab_committed(d_fan, d_b, vec![da_id.clone()], Vec::new()))
    )
    .expect("init_values is a JSON object"));
    assert!(feed(node, "diamond-r", b"x"));
    let (diamond_inst, _slot) = node
        .find_cell(&da_id)
        .expect("a durable diamond Cell is reachable by the restore handler");
    restored(&diamond_inst);
    let (outs, done) = drain(rx, "diamond-r");
    assert_eq!(done.0, "ok");
    let log = drain_diamond_log();
    assert!(
        !log.iter()
            .any(|e| e.starts_with("a:") || e.starts_with("b:")),
        "committed producers do not re-run: {log:?}"
    );
    assert_eq!(
        log.iter().filter(|e| e.starts_with("c:")).count(),
        1,
        "only the uncommitted branch re-runs"
    );
    assert_eq!(
        outs,
        vec![b"j:b:a:x".to_vec(), b"j:c:a:x".to_vec()],
        "the join runs once, with B's journaled handoff banked and C's fresh output appended"
    );
    assert_eq!(
        drain_task_commits()
            .iter()
            .map(|c| c.task_idx)
            .collect::<Vec<_>>(),
        vec![2, 3],
        "the resume commits only C and the join"
    );

    // --- steps-branching reverse-topological undo: the join is undone
    // before either producer, the shared root last ---
    let _ = drain_comp_log();
    let _ = drain_diamond_log();
    let _ = drain_task_commits();
    assert!(start_fed(node, "test-diamond-saga", "diamond-s", b"x"));
    let (_o, done) = drain(rx, "diamond-s");
    assert_eq!(done.0, "error");
    // The two branches commit on their own, so they unwind in reverse of
    // whichever finished first; the join before both, the root after both.
    let comp = drain_comp_log();
    assert_eq!(
        comp[..2],
        ["undo-j:j:b:a:x".to_string(), "undo-j:j:c:a:x".to_string()],
        "the join unwinds first"
    );
    let mut branches = comp[2..4].to_vec();
    branches.sort();
    assert_eq!(
        branches,
        ["undo-b:b:a:x".to_string(), "undo-c:c:a:x".to_string()],
        "then both branches"
    );
    assert_eq!(
        comp[4..],
        ["undo-a:a:x".to_string()],
        "the shared root last"
    );

    // --- multi-input-tasklets: the same fan-in wired as a merge. Each
    // producer's contribution reaches the join through ITS edge's mapping,
    // so a node whose Input is neither producer's Output still dispatches ---
    let _ = drain_diamond_log();
    let _ = drain_task_commits();
    assert!(start_fed(node, "test-merge-steps", "merge-1", b"x"));
    let (outs, done) = drain(rx, "merge-1");
    assert_eq!(done.0, "ok");
    assert_eq!(
        outs,
        vec![b"mb:b:a:x".to_vec(), b"mc:c:a:x".to_vec()],
        "each contribution arrives mapped by the edge it came in on"
    );
}

pub(super) fn merge_mapping(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- and the journal re-seed maps too: B's handoff is decoded with the
    // channel's codec, then lifted by the same mapping the live run used ---
    let _ = drain_diamond_log();
    let _ = drain_task_commits();
    let ma_id = crate::cell::flusher::encode_cell_identity("merge-r", "diamond_a", "");
    let merge_spec =
        crate::relay::registry::get_class("test-merge-steps").expect("merge class registered");
    let merge_topo = &merge_spec;
    let m_fan = merge_topo.lookup_channel_id("m_fan").unwrap();
    let m_b = merge_topo.lookup_channel_id("m_b").unwrap();
    assert!(start_resumable(
        node,
        "test-merge-steps".into(),
        "merge-r".into(),
        "",
        1,
        0,
        Some(ab_committed(m_fan, m_b, vec![ma_id.clone()], Vec::new()))
    )
    .expect("init_values is a JSON object"));
    assert!(feed(node, "merge-r", b"x"));
    let (merge_inst, _slot) = node
        .find_cell(&ma_id)
        .expect("a durable merge Cell is reachable by the restore handler");
    restored(&merge_inst);
    let (outs, done) = drain(rx, "merge-r");
    assert_eq!(done.0, "ok");
    assert_eq!(
        outs,
        vec![b"mb:b:a:x".to_vec(), b"mc:c:a:x".to_vec()],
        "the resumed join banks B's journaled handoff mapped, C's fresh output mapped"
    );

    // --- a panicking mapping is CONTAINED. It runs in the routing seam of
    // the producer's emit, so an unguarded unwind would strand the instance
    // before `emit_relay_completed` ---
    let _ = drain_comp_log();
    let _ = drain_diamond_log();
    let _ = drain_task_commits();
    assert!(start_fed(
        node,
        "test-panic-merge-steps",
        MERGE_PANIC_ID,
        b"x"
    ));
    let (outs, done) = drain(rx, MERGE_PANIC_ID);
    assert_eq!(
        done.0, "error",
        "the relay still REPORTS — the panic did not take the driver with it"
    );
    assert!(
        done.1.contains("edge transform panicked") && done.1.contains("merge mapping blew up"),
        "the terminal error names the mapping and carries the panic message: {:?}",
        done.1
    );
    assert!(outs.is_empty(), "the join never ran, so nothing shipped");
    // The saga undoes every stage that ran: the producer whose edge blew up
    // (B, its tasklet ran to its end), the sibling whose commit the mapping
    // waited for (C), and their parent (A) last.
    let mut comp = drain_comp_log();
    assert_eq!(
        comp.pop().as_deref(),
        Some("undo-a:a:x"),
        "the shared parent unwinds last"
    );
    comp.sort();
    assert_eq!(
        comp,
        vec!["undo-b:b:a:x".to_string(), "undo-c:c:a:x".to_string()],
        "the contained mapping panic compensates both branches that ran"
    );
    let panic_log = drain_diamond_log();
    assert!(
        panic_log.contains(&"c:a:x".to_string()),
        "the sibling ran and committed before the mapping failed: {panic_log:?}"
    );

    // --- the same containment reached WITHOUT a merge: a plain `.map` on
    // an inter-tasklet edge is the same composed closure on the same
    // routing seam ---
    let _ = drain_comp_log();
    let _ = drain_diamond_log();
    let _ = drain_task_commits();
    assert!(start_fed(
        node,
        "test-panic-transform-steps",
        "map-panic",
        b"x"
    ));
    let (outs, done) = drain(rx, "map-panic");
    assert_eq!(
        done.0, "error",
        "a panicking edge map still REPORTS — the driver thread survived"
    );
    assert!(
        done.1.contains("edge transform panicked") && done.1.contains("edge map blew up"),
        "the terminal error carries the panic message: {:?}",
        done.1
    );
    assert!(
        outs.is_empty(),
        "the consumer never ran, so nothing shipped"
    );
    assert_eq!(
        drain_comp_log(),
        vec!["undo-a:a:x".to_string()],
        "the producer whose edge blew up is inside the undo scope"
    );

    // --- the SECOND mapping site: a crash-resume re-seed. `resumed_start`
    // maps a committed producer's JOURNALED handoff into its uncommitted
    // consumers, on the driver thread and before any stage runs, so it needs
    // the same containment as the forward release — and here the undo scope
    // is the journal's own stages ---
    let _ = drain_comp_log();
    let _ = drain_diamond_log();
    let _ = drain_task_commits();
    let pa_id = crate::cell::flusher::encode_cell_identity("merge-panic-r", "diamond_a", "");
    let pb_id = crate::cell::flusher::encode_cell_identity("merge-panic-r", "diamond_b", "");
    let panic_spec = crate::relay::registry::get_class("test-panic-merge-steps")
        .expect("panic merge class registered");
    let panic_topo = &panic_spec;
    let pm_fan = panic_topo.lookup_channel_id("pm_fan").unwrap();
    let pm_b = panic_topo.lookup_channel_id("pm_b").unwrap();
    WAL_FRAMES.lock().unwrap().clear();
    assert!(start_resumable(
        node,
        "test-panic-merge-steps".into(),
        "merge-panic-r".into(),
        "",
        1,
        0,
        Some(ab_committed(
            pm_fan,
            pm_b,
            vec![pa_id.clone()],
            vec![pb_id.clone()],
        ))
    )
    .expect("init_values is a JSON object"));
    assert!(feed(node, "merge-panic-r", b"x"));
    let (panic_inst, _slot) = node
        .find_cell(&pa_id)
        .expect("a durable resumed Cell is reachable by the restore handler");
    restored(&panic_inst);
    let (outs, done) = drain(rx, "merge-panic-r");
    assert_eq!(
        done.0, "error",
        "a mapping that panics while re-seeding still REPORTS"
    );
    assert!(
        done.1.contains("edge transform panicked") && done.1.contains("merge mapping blew up"),
        "the terminal error names the mapping: {:?}",
        done.1
    );
    assert!(outs.is_empty(), "no stage ran, so nothing shipped");
    // The journaled stages ARE the undo scope here — `drive_chain` seeds the
    // resumed iteration's completed list from the journal. The comp log is
    // NOT the signal to read: `diamond_op!` logs once per completed output
    // and a journal-rebuilt CellRun carries none, so a real compensation
    // leaves it empty. The compensation-done markers the output already
    // captures are, and they carry the cell they undid.
    let done_frames: Vec<Vec<u8>> = WAL_FRAMES
        .lock()
        .unwrap()
        .iter()
        .filter(|(pid, _)| pid == DONE)
        .map(|(_, frame)| frame.clone())
        .collect();
    let carries = |frame: &[u8], id: &[u8]| frame.windows(id.len()).any(|w| w == id);
    assert_eq!(
        done_frames.len(),
        2,
        "both journaled stages were compensated"
    );
    assert!(
        carries(&done_frames[0], &pb_id) && carries(&done_frames[1], &pa_id),
        "reverse-topological: B's cell is undone before A's"
    );
}

pub(super) fn branching_resume(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- steps-branching: A→(B,C) runs to DONE — both branches receive
    // what A released on the channel they consume, and the relay is `ok`
    // exactly when every leaf has committed ---
    let _ = drain_task_commits();
    assert!(start_fed(node, "test-tree-steps", "tree-1", b"x"));
    let (mut tree_outs, done) = drain(rx, "tree-1");
    assert_eq!(done.0, "ok", "both leaves committed ⇒ the relay is done");
    tree_outs.sort();
    assert_eq!(
        tree_outs,
        vec![b"echo:x-1".to_vec(), b"x-1-2".to_vec()],
        "each branch consumed Stage1's `mid` batch and emitted on its own sink"
    );
    let mut committed: Vec<u32> = drain_task_commits().iter().map(|c| c.task_idx).collect();
    committed[1..].sort_unstable();
    assert_eq!(
        committed,
        vec![0, 1, 2],
        "the root commits before its children, which commit independently"
    );

    // --- steps-branching resume: the journal records A and B committed, so
    // only C re-runs — seeded from A's journaled edge, not from B's ---
    let _ = drain_task_commits();
    let id = "tree-resume";
    let root_seg = crate::cell::flusher::encode_cell_identity(id, "steps_stage1", "");
    // Channel ids follow declaration order: `in` = 0, `mid` = 1.
    assert!(start_resumable(
        node,
        "test-tree-steps".into(),
        id.into(),
        "",
        1,
        0,
        Some(instance::JournalResume {
            committed_task_idxs: vec![0, 1],
            task_handoffs: vec![instance::EdgeHandoff {
                task_idx: 0,
                handoff: vec![(1, postcard::to_allocvec(&"resumed-1".to_string()).unwrap())],
            }],
            committed_tasks: vec![
                instance::CommittedTask {
                    task_idx: 0,
                    iter: 0,
                    cells_ran: vec![root_seg.clone()],
                    completion: None,
                    trigger: None,
                },
                instance::CommittedTask {
                    task_idx: 1,
                    iter: 0,
                    cells_ran: Vec::new(),
                    completion: None,
                    trigger: None,
                },
            ],
            ..Default::default()
        })
    )
    .expect("init_values is a JSON object"));
    assert!(feed(node, id, b"ignored-args"));
    let (inst, _slot) = node
        .find_cell(&root_seg)
        .expect("the tree's root Cell is reachable by the restore handler");
    restored(&inst);
    let (outs, done) = drain(rx, id);
    assert_eq!(done.0, "ok");
    assert_eq!(
        outs,
        vec![b"echo:resumed-1".to_vec()],
        "only the uncommitted branch re-ran, seeded from its parent's edge"
    );
    assert_eq!(
        drain_task_commits()
            .iter()
            .map(|c| c.task_idx)
            .collect::<Vec<_>>(),
        vec![2],
        "the committed root and its committed sibling did NOT re-run"
    );

    // --- steps-branching compensation: a failure under one branch unwinds
    // BOTH branches before their shared parent (reverse topological) ---
    let _ = drain_comp_log();
    assert!(start_fed(node, "test-tree-saga", "tree-saga", b"o1"));
    let (_o, done) = drain(rx, "tree-saga");
    assert_eq!(done.0, "error");
    // The two branches commit on their own, so their undos come in whichever
    // order they completed.
    let mut undone = drain_comp_log();
    assert_eq!(
        undone.pop().as_deref(),
        Some("undo-reserve:reserved:o1"),
        "descendants before ancestors: the shared root unwinds last"
    );
    undone.sort();
    assert_eq!(
        undone,
        vec![
            "undo-archive:archived:reserved:o1".to_string(),
            "undo-charge:charged:reserved:o1".to_string(),
        ],
        "the sibling leaf and the failing branch's committed tasklet both unwind"
    );
}
