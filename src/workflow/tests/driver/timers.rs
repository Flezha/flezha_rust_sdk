//! Timers inside a workflow: a firing opens a pass at the tasklet that set it,
//! joins that tasklet's stage while it runs, or waits its turn behind the pass
//! in flight; the root's commit names it, a resume either fires it again or
//! knows it fired, and a sleeper parks naming the earliest one.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossbeam::channel::Receiver;
use flz_core::runner::RunnerNode;
use flz_core::timer::{TimerKind, TimerOp};
use flz_core::util::LockRecover;
use flz_core::workflow::instance::{CommittedTask, EdgeHandoff, FiringKey, JournalResume};

use super::super::fake_controller::*;
use super::super::fixtures::*;
use crate::runner::RunnerMessage;

pub(super) fn timers(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    let _ = drain_task_commits();
    let _ = drain_iter_commits();
    let _ = drain_armed();
    a_firing_opens_a_pass_at_the_tasklet_that_set_it(node, rx);
    a_firing_during_a_pass_waits_and_runs_before_a_later_message(node, rx);
    a_firing_joins_its_own_stage_while_it_runs(node, rx);
    a_chained_loops_carry_yields_to_a_queued_firing(node, rx);
    a_crash_before_the_roots_ack_fires_the_timer_again(node, rx);
    a_crash_after_the_roots_ack_does_not_fire_it_again(node, rx);
    a_sleeper_parks_naming_its_earliest_timer(node, rx);
}

fn a_firing_opens_a_pass_at_the_tasklet_that_set_it(
    node: &RunnerNode,
    rx: &Receiver<RunnerMessage>,
) {
    let id = "timer-open";
    let chat = start_loop(node, id);
    feed_at(&chat, b"after:30", 4);

    let outs = pump(rx, id, |outs| outs.len() == 2);
    let at = drain_armed()[0];
    assert_eq!(
        texts(&outs),
        [format!("relay:armed:{at}"), format!("relay:fired:{at}")],
        "the firing reached the sink with no second message"
    );
    settle(rx, id);
    let commits = task_commits_of(id);
    assert_eq!(
        stages(&commits),
        [
            (0, 0, false),
            (0, 1, false),
            (0, 2, false),
            (1, 1, true),
            (1, 2, false)
        ],
        "the firing's pass starts at `tick`, not at the entry channel's root"
    );
    let trigger = commits[3]
        .trigger
        .as_ref()
        .expect("the root names its firing");
    assert_eq!(
        (
            trigger.cell_key.as_str(),
            trigger.event_time,
            trigger.scheduled_at
        ),
        ("", false, at)
    );
    let iters = iter_commits_of(id);
    assert_eq!(
        iters
            .iter()
            .map(|c| (c.iter, c.consumed_offset, c.entry_channel.as_str()))
            .collect::<Vec<_>>(),
        [(0, Some(4), "in"), (1, Some(4), "in")],
        "a firing consumes no input: its commit keeps the previous offset and channel"
    );
    stop(node, rx, id);
}

fn a_firing_during_a_pass_waits_and_runs_before_a_later_message(
    node: &RunnerNode,
    rx: &Receiver<RunnerMessage>,
) {
    let id = "timer-wait";
    let chat = start_loop(node, id);
    feed_at(&chat, b"after-slow:30", 1);
    // `tick` ran and armed; its commit is held unacked while the timer comes
    // due, so the firing finds the pass in flight past its own stage.
    let _ = pump(rx, id, |_| {
        !super::super::fixtures::ARMED.lock().unwrap().is_empty()
    });
    std::thread::sleep(Duration::from_millis(80));
    feed_at(&chat, b"later", 2);

    let outs = pump(rx, id, |outs| outs.len() == 3);
    let at = drain_armed()[0];
    assert_eq!(
        texts(&outs),
        [
            format!("relay:armed-slow:{at}"),
            format!("relay:fired:{at}"),
            "relay:later".to_string()
        ],
        "the firing waited for the boundary and ran ahead of the message that came after it"
    );
    settle(rx, id);
    assert_eq!(
        iter_commits_of(id)
            .iter()
            .map(|c| (c.iter, c.consumed_offset))
            .collect::<Vec<_>>(),
        [(0, Some(1)), (1, Some(1)), (2, Some(2))]
    );
    let _ = task_commits_of(id);
    stop(node, rx, id);
}

fn a_firing_joins_its_own_stage_while_it_runs(node: &RunnerNode, rx: &Receiver<RunnerMessage>) {
    let id = "timer-join";
    let chat = start_loop(node, id);
    // The timer comes due while `tick` holds the next message's pass.
    feed_at(&chat, b"after:60", 1);
    feed_at(&chat, b"hold:250", 2);

    let outs = pump(rx, id, |outs| outs.len() == 3);
    let at = drain_armed()[0];
    assert_eq!(
        texts(&outs),
        [
            format!("relay:armed:{at}"),
            "relay:hold:250".to_string(),
            format!("relay:fired:{at}")
        ]
    );
    settle(rx, id);
    assert_eq!(
        stages(&task_commits_of(id)),
        [
            (0, 0, false),
            (0, 1, false),
            (0, 2, false),
            (1, 0, false),
            (1, 1, false),
            (1, 2, false)
        ],
        "the firing ran inside `tick`'s stage of the held pass: no pass opened for it"
    );
    assert_eq!(iter_commits_of(id).len(), 2);
    stop(node, rx, id);
}

fn a_chained_loops_carry_yields_to_a_queued_firing(
    node: &RunnerNode,
    rx: &Receiver<RunnerMessage>,
) {
    let id = "timer-chained";
    assert!(start_fed(node, "test-chained-timer-loop", id, b"3"));
    // The first iteration's commit is acked late, so the timer comes due
    // while the loop seals it, with its carry ready behind.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut held = false;
    let done = loop {
        let msg = rx
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("the chained loop never ended");
        match msg {
            RunnerMessage::EmitControlRequest {
                reference_id,
                command,
            } => {
                if !held
                    && matches!(&*command, crate::pb::command_request::Command::IterCommit(c) if c.operative_id == id)
                {
                    held = true;
                    std::thread::sleep(Duration::from_millis(60));
                }
                let _ = ack_control_request(reference_id, *command);
            }
            RunnerMessage::EmitWorkflowCompleted {
                operative_id,
                status,
                ..
            } if operative_id == id => break status,
            _ => {}
        }
    };
    assert_eq!(done, "ok", "the loop still counted down to its empty batch");
    let _ = drain_armed();
    let iters = iter_commits_of(id);
    let carried: Vec<Vec<String>> = iters
        .iter()
        .map(|c| {
            c.handoff
                .iter()
                .map(|h| postcard::from_bytes::<String>(&h.payload).unwrap())
                .collect()
        })
        .collect();
    assert_eq!(
        carried,
        [vec!["2"], vec!["2"], vec!["1"], vec!["0"], vec![]],
        "the firing's pass ran first and committed the carry as it stood; the carry fed the next"
    );
    let commits = task_commits_of(id);
    assert!(
        commits
            .iter()
            .any(|c| c.iter == 1 && c.task_idx == 0 && c.trigger.is_some()),
        "iteration 1 is the firing's"
    );
}

fn a_crash_before_the_roots_ack_fires_the_timer_again(
    node: &RunnerNode,
    rx: &Receiver<RunnerMessage>,
) {
    let id = "timer-resume-before";
    let at = now_ms() + 30;
    // The journal records no stage of the in-flight iteration: the firing's
    // root never committed.
    let journal = JournalResume {
        iter: 1,
        entry_channel: "in".into(),
        ..Default::default()
    };
    let (inst, tick) = bind_restoring(node, id, journal, at);
    flz_core::state_sink::set_secondary_stream(Some(flz_core::workflow::unwired_stream()));
    flz_core::timer::rearm_restored(&tick);
    flz_core::state_sink::set_secondary_stream(None);
    restored(&inst);

    let outs = pump(rx, id, |outs| outs.len() == 1);
    assert_eq!(texts(&outs), [format!("relay:fired:{at}")]);
    settle(rx, id);
    assert_eq!(
        stages(&task_commits_of(id)),
        [(1, 1, true), (1, 2, false)],
        "the restored timer fired again and opened the pass afresh"
    );
    stop(node, rx, id);
}

fn a_crash_after_the_roots_ack_does_not_fire_it_again(
    node: &RunnerNode,
    rx: &Receiver<RunnerMessage>,
) {
    let id = "timer-resume-after";
    let at = now_ms() + 30;
    let tick_id = crate::cell::flusher::encode_cell_identity(id, "timer_tick", "");
    let ticked = flz_core::operative::registry::get_class("test-timer-loop")
        .and_then(|t| t.lookup_channel_id("ticked"))
        .expect("the loop declares `ticked`");
    // `tick` committed the firing's stage; the crash landed before its
    // removal reached the Cell's records.
    let journal = JournalResume {
        iter: 1,
        entry_channel: "in".into(),
        committed_task_idxs: vec![1],
        task_handoffs: vec![EdgeHandoff {
            task_idx: 1,
            handoff: vec![(
                ticked,
                postcard::to_allocvec(&format!("fired:{at}")).unwrap(),
            )],
        }],
        committed_tasks: vec![CommittedTask {
            task_idx: 1,
            iter: 1,
            cells_ran: vec![tick_id],
            completion: None,
            trigger: Some(FiringKey {
                cell_key: None,
                kind: TimerKind::ProcessingTime,
                scheduled_at: at,
            }),
        }],
        ..Default::default()
    };
    let (inst, tick) = bind_restoring(node, id, journal, at);
    flz_core::timer::rearm_restored(&tick);
    assert!(
        !tick
            .timers
            .lock_recover()
            .contains(TimerKind::ProcessingTime, at),
        "a committed firing is dropped before anything re-arms it"
    );
    restored(&inst);

    let outs = pump(rx, id, |outs| outs.len() == 1);
    assert_eq!(
        texts(&outs),
        [format!("relay:fired:{at}")],
        "`relay` ran seeded from `tick`'s journaled handoff"
    );
    settle(rx, id);
    assert_eq!(
        stages(&task_commits_of(id)),
        [(1, 2, false)],
        "`tick` did not run again"
    );
    stop(node, rx, id);
}

fn a_sleeper_parks_naming_its_earliest_timer(node: &RunnerNode, rx: &Receiver<RunnerMessage>) {
    std::env::set_var("FLEZHA_PARK_IDLE_MS", "300");
    let id = "timer-park";
    let chat = start_loop(node, id);
    feed_at(&chat, b"after:90000", 1);
    let _ = pump(rx, id, |outs| outs.len() == 1);
    feed_at(&chat, b"after:60000", 2);
    let (_outs, done) = drain(rx, id);
    assert_eq!(done.0, "parked");
    let armed = drain_armed();
    let parks: Vec<_> = drain_iter_parks()
        .into_iter()
        .filter(|p| p.operative_id == id)
        .collect();
    assert_eq!(
        parks[0].wake_deadline_ms, armed[1],
        "the park's deadline is the earliest pending timer"
    );
    let _ = task_commits_of(id);
    let _ = iter_commits_of(id);
    std::env::remove_var("FLEZHA_PARK_IDLE_MS");
}

// ---- helpers ------------------------------------------------------------

fn start_loop(
    node: &RunnerNode,
    id: &str,
) -> std::sync::Arc<flz_core::operative::instance::OperativeInstance> {
    assert!(start(node, "test-timer-loop".into(), id.into(), "").expect("a JSON object"));
    instance(node, id).expect("registered")
}

/// Rebind `id` resuming at `journal` with one Cell restore owed, and put
/// the timer at `at` back on `tick`'s pending set as that restore does.
fn bind_restoring(
    node: &RunnerNode,
    id: &str,
    journal: JournalResume,
    at: u64,
) -> (
    std::sync::Arc<flz_core::operative::instance::OperativeInstance>,
    std::sync::Arc<crate::cell::CellSlot>,
) {
    assert!(start_resumable(
        node,
        "test-timer-loop".into(),
        id.into(),
        "",
        1,
        0,
        Some(journal)
    )
    .expect("a JSON object"));
    let tick_id = crate::cell::flusher::encode_cell_identity(id, "timer_tick", "");
    let (inst, tick) = find_cell(node, &tick_id).expect("`tick`'s Cell is live");
    tick.timers
        .lock_recover()
        .apply(&TimerOp::register(TimerKind::ProcessingTime, at));
    (inst, tick)
}

/// What reached `id`'s sink until `done` holds, its barriers acked as the
/// controller acks them.
fn pump(rx: &Receiver<RunnerMessage>, id: &str, done: impl Fn(&[Vec<u8>]) -> bool) -> Vec<Vec<u8>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut outs = Vec::new();
    while !done(&outs) {
        let msg = rx
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap_or_else(|_| panic!("{id} released {:?} and then nothing", texts(&outs)));
        if let RunnerMessage::EmitControlRequest {
            reference_id,
            command,
        } = msg
        {
            let for_id = control_request_operative(&command) == id;
            let flushed = ack_control_request(reference_id, *command);
            if for_id {
                outs.extend(flushed);
            }
        }
    }
    outs
}

/// Let what `id` still has in flight commit.
fn settle(rx: &Receiver<RunnerMessage>, id: &str) {
    assert_no_completion(rx, id, Duration::from_millis(200));
}

fn stop(node: &RunnerNode, rx: &Receiver<RunnerMessage>, id: &str) {
    flz_core::workflow::stop(&instance(node, id).expect("live"));
    let (_outs, done) = drain(rx, id);
    assert_eq!(done.0, "cancelled");
}

fn texts(outs: &[Vec<u8>]) -> Vec<String> {
    outs.iter()
        .map(|o| String::from_utf8_lossy(o).to_string())
        .collect()
}

/// `(iter, task_idx, names a firing)` per commit, in commit order.
fn stages(commits: &[crate::pb::TaskCommit]) -> Vec<(u32, u32, bool)> {
    commits
        .iter()
        .map(|c| (c.iter, c.task_idx, c.trigger.is_some()))
        .collect()
}

fn task_commits_of(id: &str) -> Vec<crate::pb::TaskCommit> {
    drain_task_commits()
        .into_iter()
        .filter(|c| c.operative_id == id && !c.compensating)
        .collect()
}

fn iter_commits_of(id: &str) -> Vec<crate::pb::IterCommit> {
    drain_iter_commits()
        .into_iter()
        .filter(|c| c.operative_id == id)
        .collect()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
