//! Per-Entity timers (`entity-timers`), driven through the real runtime:
//! registration and cancellation, both trigger paths, the durability of the
//! pending set, and the refusals.
//!
//! Every case that observes a firing goes through `OperativeDispatchHandle`
//! and the Entity mailbox rather than calling `on_timer` directly — the whole
//! point of the design is that a firing rides the same drain gate an input
//! does, and a test that bypassed it would not see that.

mod common;

use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use common::*;
use flezha_runner::channel::ChannelBuilder;
use flezha_runner::entity::flusher::{EffectRecordBody, SendError, SendFrameFn};
use flezha_runner::entity::header::{HeaderTemplate, FORMAT_ID_EFFECT_RECORD, FORMAT_ID_TIMER_OPS};
use flezha_runner::entity::state_sink;
use flezha_runner::entity::timer::{PendingTimers, TimerKind, TimerOp};
use flezha_runner::operative::StreamOperative;
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError, StateHandle};
use flezha_runner::state::{NoChange, RestoreInitPayload};
use flezha_runner::testing::ProcedureTestHarness;
use flezha_runner::OverflowPolicy;

/// The state sink is process-global and several cases arm real deadlines;
/// every such case takes this so they cannot clobber each other.
fn timer_test_guard() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

fn wall_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64
}

/// Restore the process-global sink to a discarding one so a recording case
/// cannot leak its buffer into whatever runs next.
fn reset_sink() {
    let sink: SendFrameFn = Arc::new(|_: &str, _: &[u8]| Ok(()));
    state_sink::install_global_sink(sink);
}

// --------------------------------------------------------------- fixtures

/// Short enough that a test waits on it, long enough that a loaded machine
/// does not fire before the case has armed its assertions.
const TIMEOUT_MS: u64 = 60;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
enum SessionChange {
    Touched(u64),
    Closed(u64),
}
impl flezha_runner::state::Change for SessionChange {}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct SessionState {
    touches: u64,
    closed_at: Vec<u64>,
}

/// One `on_timer` this suite's fixtures ran: `(entity key, kind, fired_at)`.
type Firing = (String, &'static str, u64);

static FIRINGS: OnceLock<Mutex<Vec<Firing>>> = OnceLock::new();

fn firings() -> &'static Mutex<Vec<Firing>> {
    FIRINGS.get_or_init(|| Mutex::new(Vec::new()))
}

fn firings_snapshot() -> Vec<Firing> {
    firings().lock().unwrap_or_else(|p| p.into_inner()).clone()
}

fn take_firings() -> Vec<Firing> {
    std::mem::take(&mut *firings().lock().unwrap_or_else(|p| p.into_inner()))
}

fn note_firing(key: &str, kind: TimerKind, fired_at: u64) {
    firings().lock().unwrap_or_else(|p| p.into_inner()).push((
        key.to_string(),
        kind.as_str(),
        fired_at,
    ));
}

/// The worked session-timeout shape: each input pushes the idle deadline out,
/// and the firing closes the session. Wall clock, so `timer_after` anchors on
/// real time and a due promotion is what fires it. `CAP` overrides the
/// mailbox capacity; `0` keeps the Procedure default.
#[derive(Default)]
struct SessionProc<const CAP: usize = 0> {
    key: String,
}
impl<const CAP: usize> Procedure for SessionProc<CAP> {
    type Input = String;
    type Output = String;
    type State = SessionState;
    type Change = SessionChange;
    type Clock = flezha_runner::state::WallClock;
    type Config = ();
    fn name() -> &'static str {
        "timers_session_procedure"
    }
    fn mailbox_capacity() -> usize {
        if CAP == 0 {
            Self::DEFAULT_MAILBOX_CAPACITY
        } else {
            CAP
        }
    }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = CAP != 0;
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> SessionState {
        SessionState::default()
    }
    fn apply_change(state: &mut SessionState, change: &SessionChange) {
        match change {
            SessionChange::Touched(_) => state.touches += 1,
            SessionChange::Closed(at) => state.closed_at.push(*at),
        }
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
        ctx.emit::<String>(format!("closed@{fired_at}"))?;
        Ok(())
    }
}

struct SessionOperative;
impl StreamOperative for SessionOperative {
    fn factory_id() -> &'static str {
        "timers_session_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .key_by(|s: &String| s.clone())
            .into::<SessionProc>()
            .named("out")
            .sink("sink");
    }
}

/// [`SessionOperative`] with a two-slot, `DropNothing` entry mailbox, so a
/// case can saturate it with two frames and watch a firing meet it.
struct SaturatedSessionOperative;
impl StreamOperative for SaturatedSessionOperative {
    fn factory_id() -> &'static str {
        "timers_saturated_session_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .on_full(OverflowPolicy::DropNothing)
            .key_by(|s: &String| s.clone())
            .into::<SessionProc<2>>()
            .named("out")
            .sink("sink");
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct Tick {
    at: u64,
}

struct TickTime;
impl flezha_runner::state::TimeExtractor<Tick> for TickTime {
    fn extract(input: &Tick) -> u64 {
        input.at
    }
}

/// Records the interleaving of `run` and `on_timer` so a case can assert the
/// before-the-input rule rather than infer it.
static ORDER: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

fn order() -> &'static Mutex<Vec<String>> {
    ORDER.get_or_init(|| Mutex::new(Vec::new()))
}

fn order_snapshot() -> Vec<String> {
    order().lock().unwrap_or_else(|p| p.into_inner()).clone()
}

fn take_order() -> Vec<String> {
    std::mem::take(&mut *order().lock().unwrap_or_else(|p| p.into_inner()))
}

fn note_order(entry: String) {
    order()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .push(entry);
}

/// Event-time fixture: the first input arms three timers ahead of itself; a
/// later input crosses two of them.
#[derive(Default)]
struct WindowProc;
impl Procedure for WindowProc {
    type Input = Tick;
    type Output = u64;
    type State = Vec<u64>;
    type Change = NoChange;
    type Clock = flezha_runner::state::EventTimeFrom<TickTime>;
    type Config = ();
    fn name() -> &'static str {
        "timers_window_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> Vec<u64> {
        Vec::new()
    }
    fn run(
        &mut self,
        input: Tick,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        note_order(format!("run@{}", input.at));
        if input.at == 50 {
            for at in [300u64, 100, 200] {
                ctx.timer_at_event_time(at)?;
            }
        }
        Ok(())
    }
    fn on_timer(
        &mut self,
        fired_at: u64,
        _kind: TimerKind,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        note_order(format!("timer@{fired_at}"));
        Ok(())
    }
}

struct WindowOperative;
impl StreamOperative for WindowOperative {
    fn factory_id() -> &'static str {
        "timers_window_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Tick>("in").into::<WindowProc>();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SurfaceCmd {
    Register,
    Cancel,
    RegisterMany(usize),
    RegisterEventTime,
}

#[derive(Default)]
struct SurfaceProc;
impl Procedure for SurfaceProc {
    type Input = SurfaceCmd;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = flezha_runner::state::WallClock;
    type Config = ();
    fn name() -> &'static str {
        "timers_surface_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn max_timers_per_entity() -> usize {
        3
    }
    fn run(
        &mut self,
        input: SurfaceCmd,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        match input {
            SurfaceCmd::Register => {
                let at = ctx.timer_after(Duration::from_secs(30))?;
                assert_eq!(at, 31_000, "the call returns the absolute schedule");
            }
            SurfaceCmd::Cancel => {
                assert!(ctx.cancel_processing_time_timer(31_000)?);
            }
            SurfaceCmd::RegisterMany(n) => {
                for i in 0..n {
                    ctx.timer_after(Duration::from_secs(30 + i as u64))?;
                }
            }
            SurfaceCmd::RegisterEventTime => {
                ctx.timer_at_event_time(5_000)?;
            }
        }
        Ok(())
    }
    fn on_timer(
        &mut self,
        _fired_at: u64,
        _kind: TimerKind,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Ok(())
    }
}

// -------------------------------------------------- surface and refusals

/// Spec: "A timer is registered and returns its absolute time" + "Duplicate
/// registration is idempotent" + "Cancellation removes a pending timer".
#[test]
fn the_registration_surface_is_absolute_idempotent_and_cancellable() {
    let mut harness = ProcedureTestHarness::<SurfaceProc>::new().with_wall_clock(1_000);
    harness.apply_inputs(vec![SurfaceCmd::Register]);
    assert_eq!(
        harness.pending_timers(TimerKind::ProcessingTime),
        vec![31_000],
        "timer_after(30s) at now=1000 is scheduled at 31000"
    );

    harness.apply_inputs(vec![SurfaceCmd::Register]);
    assert_eq!(
        harness.pending_timers(TimerKind::ProcessingTime),
        vec![31_000],
        "pending timers are a set: re-registering the same instant is a no-op"
    );
    assert_eq!(
        harness.recorded_timer_ops().len(),
        1,
        "and the no-op produces no second durable op"
    );

    harness.apply_inputs(vec![SurfaceCmd::Cancel]);
    assert!(harness.pending_timers(TimerKind::ProcessingTime).is_empty());
}

/// Spec: "Registration past the cap is refused" — naming the Procedure and the
/// bound, leaving the pending set unchanged.
#[test]
fn registration_past_the_cap_is_refused_naming_the_procedure_and_the_bound() {
    let err = refusal_of(SurfaceCmd::RegisterMany(4));
    assert!(
        err.contains("timers_surface_procedure") && err.contains('3'),
        "the refusal names the procedure and the bound: {err}"
    );
}

/// Spec: "An event-time timer without an event-time clock is refused" —
/// naming the Procedure and its declared clock, registering nothing.
#[test]
fn an_event_time_timer_under_a_wall_clock_is_refused_naming_the_clock() {
    let err = refusal_of(SurfaceCmd::RegisterEventTime);
    assert!(
        err.contains("timers_surface_procedure") && err.contains("WallClock"),
        "the refusal names the procedure and its clock: {err}"
    );
}

/// Drive one input expecting `run` to fail; the harness panics on `Err`, which
/// is exactly the observation a refusal case wants.
fn refusal_of(input: SurfaceCmd) -> String {
    let payload = std::panic::catch_unwind(move || {
        ProcedureTestHarness::<SurfaceProc>::new()
            .with_wall_clock(1_000)
            .apply_inputs(vec![input]);
    })
    .expect_err("the refusal must surface");
    if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else {
        String::from("<non-string panic>")
    }
}

// ------------------------------------------------------- event-time firing

/// Spec: "Crossing the high-water fires in order" — 100 then 200, both BEFORE
/// the input that crossed them, with 300 still pending.
#[test]
fn crossing_the_high_water_fires_in_order_before_the_input() {
    let _guard = timer_test_guard();
    take_order();

    let (handle, instance) = setup::<WindowOperative>("timers-window-order");
    let stream = dummy_stream();
    dispatch_input(&handle, "timers-window-order", "in", tick(50), &stream);
    assert!(wait_until(
        || order_snapshot().len() == 1,
        Duration::from_secs(5)
    ));
    dispatch_input(&handle, "timers-window-order", "in", tick(250), &stream);

    assert!(
        wait_until(|| order_snapshot().len() >= 4, Duration::from_secs(5)),
        "the two firings and the two runs must all land, got {:?}",
        order_snapshot()
    );
    assert_eq!(
        take_order(),
        vec![
            "run@50".to_string(),
            "timer@100".to_string(),
            "timer@200".to_string(),
            "run@250".to_string(),
        ]
    );
    assert_eq!(
        pending(&instance.entities[0][0], TimerKind::EventTime),
        vec![300],
        "300 is beyond the new high-water and stays pending"
    );
}

/// Spec: "A quiet Entity does not fire" — event time is a property of the
/// input, so an Entity nothing arrives at has no clock to cross.
#[test]
fn a_quiet_entity_never_fires_its_event_time_timer() {
    let _guard = timer_test_guard();
    take_order();

    let (handle, instance) = setup::<WindowOperative>("timers-window-quiet");
    let stream = dummy_stream();
    dispatch_input(&handle, "timers-window-quiet", "in", tick(50), &stream);
    assert!(wait_until(
        || pending(&instance.entities[0][0], TimerKind::EventTime).len() == 3,
        Duration::from_secs(5)
    ));

    // Wall time passes; nothing arrives.
    assert!(stays_quiet(
        || order_snapshot().len() > 1,
        Duration::from_millis(200)
    ));

    assert_eq!(
        take_order(),
        vec!["run@50".to_string()],
        "no firing, however much wall-clock time passes"
    );
}

// -------------------------------------------- processing-time firing

/// The processing-time path end to end: an input arms a deadline, nothing
/// else arrives, and `on_timer` runs with the SCHEDULED timestamp — not the
/// wall clock at which a poller got round to promoting it. Also spec "Timers
/// are per Entity": only the key that registered fires.
#[test]
fn a_processing_time_timer_fires_from_the_wake_list_with_its_scheduled_time() {
    let _guard = timer_test_guard();
    take_firings();

    let (handle, _instance) = setup::<SessionOperative>("timers-session-fire");
    let stream = dummy_stream();
    let before = wall_now_ms();
    dispatch_input(
        &handle,
        "timers-session-fire",
        "in",
        b"alice".to_vec(),
        &stream,
    );

    assert!(
        wait_until(|| !firings_snapshot().is_empty(), Duration::from_secs(5)),
        "the deadline must fire"
    );
    // A stray second firing gets its chance to appear and does not — a
    // per-Entity timer must not fan out.
    assert!(stays_quiet(
        || firings_snapshot().len() > 1,
        Duration::from_millis(100)
    ));
    let fired = take_firings();
    assert_eq!(fired.len(), 1, "one Entity, one timer, one firing");
    let (key, kind, fired_at) = &fired[0];
    assert_eq!(key, "alice", "the firing carries the registering Entity");
    assert_eq!(*kind, "processing_time");
    let after = wall_now_ms();
    assert!(
        *fired_at >= before + TIMEOUT_MS && *fired_at <= after,
        "fired_at is the scheduled instant (now + {TIMEOUT_MS}ms), not the wall clock at firing"
    );
}

/// Spec (`sdk-poll-core`) "A due wake that cannot be delivered stays on the
/// wake list": the poller promoting a due timer neither serves the saturated
/// consumer inline nor parks on it. It re-records the entry and returns, and
/// the firing lands on a later pass once room appears — the firing is delayed,
/// never dropped, and no thread is held inside `poll` while it waits.
#[test]
fn a_firing_into_a_saturated_consumer_waits_for_room_without_holding_a_poller() {
    let _guard = timer_test_guard();
    take_firings();
    let (sock, _peer) = std::os::unix::net::UnixStream::pair().expect("pair");
    state_sink::set_secondary_stream(Some(Arc::new(
        flezha_runner::transport::FlezhaStream::Unix(sock),
    )));

    let (handle, instance) = setup::<SaturatedSessionOperative>("timers-saturated");
    let registry = Arc::clone(instance.dynamic_registries[0].as_ref().expect("dynamic"));
    let slot = registry.get_or_create("alice").expect("entity");
    let stream = dummy_stream();

    // Inflight held with no drainer, then both slots filled: nothing pops
    // until whoever pushes next serves the round.
    assert!(slot.mailbox.claim_inflight());
    for _ in 0..2 {
        dispatch_input(
            &handle,
            "timers-saturated",
            "in",
            b"alice".to_vec(),
            &stream,
        );
    }
    assert!(slot.mailbox.is_full());

    // A deadline already in the past: the next poll pass promotes it.
    slot.timers
        .lock()
        .unwrap()
        .apply(&TimerOp::register(TimerKind::ProcessingTime, 1));
    flezha_runner::entity::timer::rearm_restored(&slot);

    // Held, not delivered and not lost: with no room the promotion pass keeps
    // re-recording the entry. (The retired behaviour ran the round inline here
    // and the firing landed with the mailbox still full.)
    assert!(
        stays_quiet(
            || !firings_snapshot().is_empty(),
            Duration::from_millis(200)
        ),
        "a firing must not be delivered into a mailbox with no room"
    );
    assert!(slot.mailbox.is_full(), "the mailbox is still saturated");

    // Room appears and the round gets a drainer: the deferred firing lands.
    assert!(slot.mailbox.pop().is_some());
    slot.mailbox.release_inflight();
    dispatch_input(
        &handle,
        "timers-saturated",
        "in",
        b"alice".to_vec(),
        &stream,
    );

    assert!(
        wait_until(|| !firings_snapshot().is_empty(), Duration::from_secs(5)),
        "the deferred firing must land once the mailbox has room"
    );

    // The two queued inputs re-armed the deadline; let it fire before the
    // next case counts firings.
    take_firings();
    assert!(wait_until(
        || !firings_snapshot().is_empty(),
        Duration::from_millis(TIMEOUT_MS * 3)
    ));
    take_firings();
    state_sink::set_secondary_stream(None);
}

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

// ------------------------------------------------------------ eviction

/// Spec: "Eviction discards the Entity's timers" — dynamic eviction is
/// destructive by existing decision, so the timers go with the state and the
/// wake-list entry goes with them. A later input cold-starts with nothing
/// pending.
#[test]
fn an_idle_evicted_entity_loses_its_timers() {
    let _guard = timer_test_guard();
    take_firings();
    let armed_before = flezha_runner::entity::timer::timers_armed();

    let (handle, instance) = setup::<EvictOperative>("timers-evict");
    let stream = dummy_stream();
    dispatch_input(&handle, "timers-evict", "in", b"gone".to_vec(), &stream);

    let registry = Arc::clone(instance.dynamic_registries[0].as_ref().expect("dynamic"));
    assert!(
        wait_until(
            || registry.entities.get("gone").is_some_and(|e| !e
                .value()
                .timers
                .lock()
                .unwrap()
                .is_empty()),
            Duration::from_secs(5)
        ),
        "the Entity must arm its timer before the reaper runs"
    );
    assert_eq!(
        flezha_runner::entity::timer::timers_armed(),
        armed_before + 1
    );

    // Sweep directly rather than waiting on the reaper's cadence: the seam
    // under test is what the sweep does, not when it decides to run.
    assert!(
        wait_until(
            || {
                registry.reap_sweep(Duration::from_millis(1));
                registry.entities.get("gone").is_none()
            },
            Duration::from_secs(5)
        ),
        "the Entity is evicted"
    );
    assert_eq!(
        flezha_runner::entity::timer::timers_armed(),
        armed_before,
        "and its armed timer is released with it, not left to fire into nothing"
    );
    assert!(
        firings_snapshot().is_empty(),
        "an evicted Entity's timer never fires"
    );
}

/// Dynamic keying + a TTL: the combination the design calls out as legal but
/// timer-disposable.
#[derive(Default)]
struct EvictProc;
impl Procedure for EvictProc {
    type Input = String;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = flezha_runner::state::WallClock;
    type Config = ();
    fn name() -> &'static str {
        "timers_evict_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn entity_idle_ttl() -> Option<Duration> {
        // Long enough that the reaper thread never beats the test to the
        // sweep; the case drives `reap_sweep` itself with a short TTL.
        Some(Duration::from_secs(3_600))
    }
    fn run(
        &mut self,
        _input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.timer_after(Duration::from_secs(3_600))?;
        Ok(())
    }
    fn on_timer(
        &mut self,
        fired_at: u64,
        kind: TimerKind,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        note_firing("gone", kind, fired_at);
        Ok(())
    }
}

struct EvictOperative;
impl StreamOperative for EvictOperative {
    fn factory_id() -> &'static str {
        "timers_evict_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .key_by(|s: &String| s.clone())
            .into::<EvictProc>();
    }
}

// ------------------------------------------------------ exclusion

/// Spec: "Firing is serialized against run" — the mailbox's single-active-
/// drainer gate is the whole mechanism, so a firing that expires mid-`run`
/// waits for it. The fixture would observe an overlap if one were possible.
#[test]
fn on_timer_never_runs_concurrently_with_run() {
    let _guard = timer_test_guard();
    OVERLAPS.store(0, Ordering::SeqCst);
    INSIDE.store(0, Ordering::SeqCst);
    FIRED.store(0, Ordering::SeqCst);

    let (handle, _instance) = setup::<ExclusionOperative>("timers-exclusion");
    let stream = dummy_stream();
    for i in 0..8u64 {
        dispatch_input(
            &handle,
            "timers-exclusion",
            "in",
            serde_json::to_vec(&i).unwrap(),
            &stream,
        );
    }

    assert!(
        wait_until(
            || FIRED.load(Ordering::SeqCst) >= 4,
            Duration::from_secs(10)
        ),
        "expected several firings to interleave with the runs, saw {}",
        FIRED.load(Ordering::SeqCst)
    );
    assert_eq!(
        OVERLAPS.load(Ordering::SeqCst),
        0,
        "run and on_timer must never be inside the same Entity at once"
    );
}

static INSIDE: AtomicI64 = AtomicI64::new(0);
static OVERLAPS: AtomicUsize = AtomicUsize::new(0);
static FIRED: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct ExclusionProc;
impl ExclusionProc {
    fn enter() {
        if INSIDE.fetch_add(1, Ordering::SeqCst) != 0 {
            OVERLAPS.fetch_add(1, Ordering::SeqCst);
        }
        // Hold the section long enough for a concurrent entry to overlap it.
        let held = Instant::now();
        while held.elapsed() < Duration::from_millis(5) {
            std::hint::spin_loop();
        }
    }
    fn leave() {
        INSIDE.fetch_sub(1, Ordering::SeqCst);
    }
}
impl Procedure for ExclusionProc {
    type Input = u64;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = flezha_runner::state::WallClock;
    type Config = ();
    fn name() -> &'static str {
        "timers_exclusion_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _input: u64,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Self::enter();
        ctx.timer_after(Duration::from_millis(1))?;
        Self::leave();
        Ok(())
    }
    fn on_timer(
        &mut self,
        _fired_at: u64,
        _kind: TimerKind,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Self::enter();
        FIRED.fetch_add(1, Ordering::SeqCst);
        Self::leave();
        Ok(())
    }
}

struct ExclusionOperative;
impl StreamOperative for ExclusionOperative {
    fn factory_id() -> &'static str {
        "timers_exclusion_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<u64>("in").into::<ExclusionProc>();
    }
}

// ------------------------------------------------------------- helpers

fn tick(at: u64) -> Vec<u8> {
    serde_json::to_vec(&Tick { at }).expect("encode tick")
}

fn pending(slot: &Arc<flezha_runner::entity::EntitySlot>, kind: TimerKind) -> Vec<u64> {
    slot.timers
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .all(kind)
}

/// Build one controller-framed WAL record carrying timer ops
/// (`format_id = 3`), the shape `init_entity_with_payload` folds.
fn timer_ops_record<S: Procedure>(seq: u64, ops: &[TimerOp]) -> Vec<u8> {
    let body = postcard::to_allocvec(ops).expect("encode ops");
    wal_record(
        seq,
        &body,
        HeaderTemplate {
            format_id: FORMAT_ID_TIMER_OPS,
            ..flezha_runner::entity::header::header_template_for::<S>()
        },
    )
}

/// Build one controller-framed effect record carrying both Changes and timer
/// ops — the atomic shape a tier firing produces.
fn effect_record<S: Procedure>(seq: u64, changes: Vec<Vec<u8>>, timers: Vec<TimerOp>) -> Vec<u8> {
    let body = postcard::to_allocvec(&EffectRecordBody {
        changes,
        outputs: Vec::new(),
        consumed_offset: None,
        dlq: None,
        event_time: 0,
        timers,
    })
    .expect("encode effect body");
    wal_record(
        seq,
        &body,
        HeaderTemplate {
            format_id: FORMAT_ID_EFFECT_RECORD,
            ..flezha_runner::entity::header::header_template_for::<S>()
        },
    )
}

fn wal_record(seq: u64, body: &[u8], template: HeaderTemplate) -> Vec<u8> {
    let header = template.stamp(body);
    let mut record = Vec::new();
    record.extend_from_slice(&seq.to_be_bytes());
    record.extend_from_slice(&((16 + body.len()) as u32).to_be_bytes());
    record.extend_from_slice(&header);
    record.extend_from_slice(body);
    let mut crc_input = header.to_vec();
    crc_input.extend_from_slice(body);
    record.extend_from_slice(&crc32fast::hash(&crc_input).to_be_bytes());
    record
}

fn restore<S: Procedure>(change_log: &[u8]) -> (S::State, PendingTimers)
where
    S::State: serde::de::DeserializeOwned,
{
    let (state, timers, _migrated) = flezha_runner::entity::restore::init_entity_with_payload::<S>(
        None,
        Some(&RestoreInitPayload {
            snapshot_header: &[],
            snapshot_bytes: &[],
            change_log_bytes: change_log,
        }),
    )
    .expect("restore");
    (state, timers)
}

/// Every state frame the runtime shipped, as `(payload_id, frame)`.
type ShippedFrames = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

/// Install a sink that records every state frame the runtime ships.
fn recording_sink() -> ShippedFrames {
    let seen: ShippedFrames = Arc::new(Mutex::new(Vec::new()));
    let seen_c = Arc::clone(&seen);
    let sink: SendFrameFn = Arc::new(move |payload_id: &str, frame: &[u8]| {
        seen_c
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push((payload_id.to_string(), frame.to_vec()));
        Ok::<(), SendError>(())
    });
    state_sink::install_global_sink(sink);
    seen
}

fn frame_parts(frame: &[u8]) -> (u64, u8, Vec<u8>) {
    let pid_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
    let seq_off = 2 + pid_len;
    let seq = u64::from_be_bytes(frame[seq_off..seq_off + 8].try_into().unwrap());
    let header_off = seq_off + 8;
    let format_id = frame[header_off + 9];
    let body_len_off = header_off + 16;
    let body_len =
        u32::from_be_bytes(frame[body_len_off..body_len_off + 4].try_into().unwrap()) as usize;
    let body = frame[body_len_off + 4..body_len_off + 4 + body_len].to_vec();
    (seq, format_id, body)
}

fn frame_format_id(frame: &[u8]) -> u8 {
    frame_parts(frame).1
}

fn decode_timer_frame(frame: &[u8]) -> (u64, Vec<TimerOp>) {
    let (seq, _fmt, body) = frame_parts(frame);
    (seq, postcard::from_bytes(&body).expect("decode timer ops"))
}

fn effect_bodies(frames: &ShippedFrames) -> Vec<EffectRecordBody> {
    frames
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|(_, f)| frame_format_id(f) == FORMAT_ID_EFFECT_RECORD)
        .map(|(_, f)| postcard::from_bytes(&frame_parts(f).2).expect("decode effect body"))
        .collect()
}
