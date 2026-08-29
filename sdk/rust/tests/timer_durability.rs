//! The pending set is folded state: it reaches the WAL as timer ops, and a
//! replay rebuilds it exactly, overdue entries included.

use std::sync::Arc;
use std::time::Duration;

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::entity::header::FORMAT_ID_TIMER_OPS;
use flezha_runner::entity::state_sink;
use flezha_runner::entity::timer::{TimerKind, TimerOp};
use flezha_runner::operative::StreamOperative;
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError, StateHandle};
use flezha_runner::state::{NoChange, RestoreInitPayload};
use flezha_runner::testing::ProcedureTestHarness;

mod common;
use common::*;

// ------------------------------------------------------------ durability

/// Spec: "A crash before firing leaves the timer pending" — the pending set is
/// folded out of the same change log the state is, so a restore owes exactly
/// the timers the crash interrupted.
#[test]
fn a_crash_before_firing_leaves_the_timer_pending() {
    let log =
        timer_ops_record::<SessionProc>(0, &[TimerOp::register(TimerKind::ProcessingTime, 31_000)]);
    let (_state, timers) = restore::<SessionProc>(&log);
    assert_eq!(timers.all(TimerKind::ProcessingTime), vec![31_000]);
}

/// Spec: "A crash after firing does not re-fire" — the removal is a durable
/// record of its own, folded in the same walk.
#[test]
fn a_crash_after_firing_does_not_re_fire() {
    let mut log =
        timer_ops_record::<SessionProc>(0, &[TimerOp::register(TimerKind::ProcessingTime, 31_000)]);
    log.extend_from_slice(&timer_ops_record::<SessionProc>(
        1,
        &[TimerOp::cancel(TimerKind::ProcessingTime, 31_000)],
    ));
    let (_state, timers) = restore::<SessionProc>(&log);
    assert!(
        timers.is_empty(),
        "the fired timer is gone; nothing re-fires"
    );
}

/// Spec: "A crash mid-firing replays the whole firing" — the firing's Changes
/// and its removal are ONE effect record, so a crash before it is durable
/// leaves the timer pending AND the state untouched. There is no in-between.
#[test]
fn a_crash_mid_firing_leaves_no_partial_state_and_the_timer_pending() {
    let registered =
        timer_ops_record::<SessionProc>(0, &[TimerOp::register(TimerKind::ProcessingTime, 31_000)]);
    let (state, timers) = restore::<SessionProc>(&registered);
    assert_eq!(state.closed_at, Vec::<u64>::new());
    assert_eq!(timers.all(TimerKind::ProcessingTime), vec![31_000]);

    let mut both = registered.clone();
    both.extend_from_slice(&effect_record::<SessionProc>(
        1,
        vec![postcard::to_allocvec(&SessionChange::Closed(31_000)).unwrap()],
        vec![TimerOp::cancel(TimerKind::ProcessingTime, 31_000)],
    ));
    let (state, timers) = restore::<SessionProc>(&both);
    assert_eq!(state.closed_at, vec![31_000]);
    assert!(timers.is_empty());
}

/// A compaction truncates every record the snapshot folds — so the pending
/// timers are re-stated ABOVE the snapshot seq, or a compacted Entity silently
/// forgets what it owes.
#[test]
fn a_compaction_re_states_the_pending_timers_above_the_snapshot_seq() {
    let _guard = timer_test_guard();
    take_order();
    let frames = recording_sink();

    let (handle, instance) = setup::<WindowOperative>("timers-compaction");
    let stream = dummy_stream();
    dispatch_input(&handle, "timers-compaction", "in", tick(50), &stream);
    let slot = Arc::clone(&instance.entities[0][0]);
    assert!(wait_until(
        || pending(&slot, TimerKind::EventTime).len() == 3,
        Duration::from_secs(5)
    ));
    frames.lock().unwrap().clear();

    let (_frame, snapshot_seq, _size) = {
        let mut state = slot.state.lock().unwrap();
        let mut buffer = slot.flush_buffer.lock().unwrap();
        let timers = slot.timers.lock().unwrap();
        slot.dispatcher
            .snapshot_frame(&mut state, &mut buffer, &timers)
            .expect("snapshot")
    };

    let restated = frames.lock().unwrap().clone();
    let timer_frames: Vec<&(String, Vec<u8>)> = restated
        .iter()
        .filter(|(_, f)| frame_format_id(f) == FORMAT_ID_TIMER_OPS)
        .collect();
    assert_eq!(
        timer_frames.len(),
        1,
        "the compaction re-states the set exactly once"
    );
    let (seq, ops) = decode_timer_frame(&timer_frames[0].1);
    assert!(
        seq > snapshot_seq,
        "the re-statement must survive truncate_up_to({snapshot_seq}), so seq {seq} > it"
    );
    assert_eq!(
        ops,
        vec![
            TimerOp::register(TimerKind::EventTime, 100),
            TimerOp::register(TimerKind::EventTime, 200),
            TimerOp::register(TimerKind::EventTime, 300),
        ]
    );
    reset_sink();
}

/// Outside the effectively-once tier a registration still becomes durable —
/// as its own `format_id = 3` record, which the controller stores exactly like
/// a change record and never decodes.
#[test]
fn a_registration_outside_the_tier_ships_a_timer_ops_record() {
    let _guard = timer_test_guard();
    take_order();
    let frames = recording_sink();

    let (handle, instance) = setup::<WindowOperative>("timers-record");
    let stream = dummy_stream();
    dispatch_input(&handle, "timers-record", "in", tick(50), &stream);
    assert!(wait_until(
        || pending(&instance.entities[0][0], TimerKind::EventTime).len() == 3,
        Duration::from_secs(5)
    ));
    assert!(wait_until(
        || frames
            .lock()
            .unwrap()
            .iter()
            .any(|(_, f)| frame_format_id(f) == FORMAT_ID_TIMER_OPS),
        Duration::from_secs(5)
    ));

    let seen = frames.lock().unwrap().clone();
    let (pid, frame) = seen
        .iter()
        .find(|(_, f)| frame_format_id(f) == FORMAT_ID_TIMER_OPS)
        .expect("a timer-ops record");
    assert_eq!(
        pid, "entity_state_change",
        "it rides the payload-id the controller already stores verbatim"
    );
    let (_seq, ops) = decode_timer_frame(frame);
    assert_eq!(
        ops,
        vec![
            TimerOp::register(TimerKind::EventTime, 300),
            TimerOp::register(TimerKind::EventTime, 100),
            TimerOp::register(TimerKind::EventTime, 200),
        ],
        "ops are recorded in registration order — the fold is order-sensitive"
    );
    reset_sink();
}

/// Spec: "A firing produces a nil-offset effect record" — under the tier the
/// firing's Changes, outputs and the removal are ONE record carrying NO
/// consumed offset, because a firing consumes no input.
#[test]
fn a_tier_firing_writes_one_record_with_no_consumed_offset() {
    let _guard = timer_test_guard();
    take_firings();
    let frames = recording_sink();

    let (handle, _instance) = setup::<TierSessionOperative>("timers-tier");
    let stream = dummy_stream();
    dispatch_marked(
        &handle,
        "timers-tier",
        "in",
        b"alice".to_vec(),
        &stream,
        &[("flz.offset", "7")],
    );

    assert!(
        wait_until(|| !firings_snapshot().is_empty(), Duration::from_secs(5)),
        "the deadline must fire"
    );
    assert!(
        wait_until(
            || effect_bodies(&frames)
                .iter()
                .any(|b| !b.timers.is_empty() && !b.timers[0].register),
            Duration::from_secs(5)
        ),
        "the firing's record must reach the sink"
    );

    let bodies = effect_bodies(&frames);
    assert_eq!(
        bodies[0].consumed_offset,
        Some(7),
        "the input's record acknowledges its offset"
    );
    let firing = bodies
        .iter()
        .find(|b| !b.timers.is_empty() && !b.timers[0].register)
        .expect("the firing's record");
    assert_eq!(
        firing.consumed_offset, None,
        "a firing consumes no input offset, so the watermark cannot advance on it"
    );
    assert_eq!(firing.changes.len(), 1, "the Change `on_timer` applied");
    assert_eq!(firing.outputs.len(), 1, "and the value it emitted");
    assert_eq!(
        firing.timers.len(),
        1,
        "the removal rides the SAME record — never a second one"
    );
    take_firings();
    reset_sink();
}

/// The tier twin of [`SessionProc`]: same shape, keyed output so its emits
/// ride the effect record.
#[derive(Default)]
struct TierSessionProc {
    key: String,
}
impl Procedure for TierSessionProc {
    type Input = String;
    type Output = String;
    type State = SessionState;
    type Change = SessionChange;
    type Clock = flezha_runner::state::WallClock;
    type Config = ();
    fn name() -> &'static str {
        "timers_tier_session_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> SessionState {
        SessionState::default()
    }
    fn apply_change(state: &mut SessionState, change: &SessionChange) {
        <SessionProc as Procedure>::apply_change(state, change)
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        self.key = input.clone();
        state.apply(SessionChange::Touched(ctx.now()));
        ctx.timer_after(Duration::from_millis(TIMEOUT_MS))?;
        Ok(())
    }
    fn on_timer(
        &mut self,
        fired_at: u64,
        kind: TimerKind,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        note_firing(&self.key, kind, fired_at);
        state.apply(SessionChange::Closed(fired_at));
        ctx.emit_keyed::<String>("k".to_string(), format!("closed@{fired_at}"))?;
        Ok(())
    }
}

struct TierSessionOperative;
impl StreamOperative for TierSessionOperative {
    fn factory_id() -> &'static str {
        "timers_tier_session_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.effectively_once();
        c.input::<String>("in")
            .into::<TierSessionProc>()
            .named("out")
            .keyed()
            .sink("sink");
    }
}

// ------------------------------------------------------------- replay

/// Spec: "Replay registers the same timers" — the pending set is folded state,
/// so `replay_equals` (which covers it) is the gate. A registration is NOT
/// skipped on replay, unlike `spawn_workflow`.
#[test]
fn replay_rebuilds_the_identical_pending_set() {
    let mut harness = ProcedureTestHarness::<SurfaceProc>::new().with_wall_clock(1_000);
    harness.apply_inputs(vec![
        SurfaceCmd::RegisterMany(2),
        SurfaceCmd::Register,
        SurfaceCmd::Cancel,
    ]);
    assert_eq!(
        harness.pending_timers(TimerKind::ProcessingTime),
        vec![32_000],
        "31000 was registered twice then cancelled; 32000 survives"
    );
    harness
        .replay_equals()
        .expect("the folded ops rebuild the same pending set");
}

/// Spec: "on_timer may register a further timer" — the chained registration is
/// anchored on `fired_at`, so a periodic tick is exactly reproducible.
#[test]
fn on_timer_may_chain_a_further_timer_anchored_on_fired_at() {
    let mut harness = ProcedureTestHarness::<ChainProc>::new().with_wall_clock(1_000);
    harness.apply_inputs(vec![()]);
    assert_eq!(
        harness.pending_timers(TimerKind::ProcessingTime),
        vec![1_100]
    );
    harness.fire_timer(TimerKind::ProcessingTime, 1_100);
    assert_eq!(
        harness.pending_timers(TimerKind::ProcessingTime),
        vec![1_200],
        "the chained timer is fired_at + 100, not wall + 100"
    );
    harness.replay_equals().expect("the chained set replays");
}

#[derive(Default)]
struct ChainProc;
impl Procedure for ChainProc {
    type Input = ();
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = flezha_runner::state::WallClock;
    type Config = ();
    fn name() -> &'static str {
        "timers_chain_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _input: (),
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.timer_after(Duration::from_millis(100))?;
        Ok(())
    }
    fn on_timer(
        &mut self,
        _fired_at: u64,
        _kind: TimerKind,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.timer_after(Duration::from_millis(100))?;
        Ok(())
    }
}

/// Spec: "A due timer fires on restore" — the pending set comes back through
/// the fold, the wake list is re-armed from it, and anything already past its
/// deadline fires at once, ascending.
#[test]
fn a_restore_re_arms_the_wake_list_and_fires_what_is_already_due() {
    let _guard = timer_test_guard();
    take_firings();
    let (sock, _peer) = std::os::unix::net::UnixStream::pair().expect("pair");
    state_sink::set_secondary_stream(Some(Arc::new(
        flezha_runner::transport::FlezhaStream::Unix(sock),
    )));

    let (_handle, instance) = setup::<SessionOperative>("timers-restore");
    let registry = Arc::clone(instance.dynamic_registries[0].as_ref().expect("dynamic"));
    let slot = registry.get_or_create("carol").expect("entity");

    // Stand in for the restore's fold: two deadlines already in the past.
    {
        let mut timers = slot.timers.lock().unwrap();
        timers.apply(&TimerOp::register(TimerKind::ProcessingTime, 1));
        timers.apply(&TimerOp::register(TimerKind::ProcessingTime, 2));
    }
    flezha_runner::entity::timer::rearm_restored(&slot);

    assert!(
        wait_until(|| firings_snapshot().len() == 2, Duration::from_secs(5)),
        "both overdue timers must fire, saw {:?}",
        firings_snapshot()
    );
    let fired: Vec<u64> = take_firings().into_iter().map(|(_, _, at)| at).collect();
    assert_eq!(fired, vec![1, 2], "ascending scheduled order");
    state_sink::set_secondary_stream(None);
}
