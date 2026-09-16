//! Per-Entity timer fixtures: the classes the timer suites arm deadlines on,
//! the process-global sink guard they share, and the readers that turn a
//! pending set, a WAL record and a captured frame back into assertions.

use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

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

use super::*;

/// The state sink is process-global and several cases arm real deadlines;
/// every such case takes this so they cannot clobber each other.
pub fn timer_test_guard() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

pub fn wall_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64
}

/// Restore the process-global sink to a discarding one so a recording case
/// cannot leak its buffer into whatever runs next.
pub fn reset_sink() {
    let sink: SendFrameFn = Arc::new(|_: &str, _: &[u8]| Ok(()));
    state_sink::install_global_sink(sink);
}

// --------------------------------------------------------------- fixtures

/// Short enough that a test waits on it, long enough that a loaded machine
/// does not fire before the case has armed its assertions.
pub const TIMEOUT_MS: u64 = 60;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SessionChange {
    Touched(u64),
    Closed(u64),
}
impl flezha_runner::state::Change for SessionChange {}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SessionState {
    pub touches: u64,
    pub closed_at: Vec<u64>,
}

/// One `on_timer` this suite's fixtures ran: `(entity key, kind, fired_at)`.
pub type Firing = (String, &'static str, u64);

pub static FIRINGS: OnceLock<Mutex<Vec<Firing>>> = OnceLock::new();

pub fn firings() -> &'static Mutex<Vec<Firing>> {
    FIRINGS.get_or_init(|| Mutex::new(Vec::new()))
}

pub fn firings_snapshot() -> Vec<Firing> {
    firings().lock().unwrap_or_else(|p| p.into_inner()).clone()
}

pub fn take_firings() -> Vec<Firing> {
    std::mem::take(&mut *firings().lock().unwrap_or_else(|p| p.into_inner()))
}

pub fn note_firing(key: &str, kind: TimerKind, fired_at: u64) {
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
pub struct SessionProc<const CAP: usize = 0> {
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

pub struct SessionOperative;
impl StreamOperative for SessionOperative {
    fn factory_id() -> &'static str {
        "timers_session_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .key_by(|s: &String| s.clone())
            .into::<SessionProc>()
            .sink_named("out");
    }
}

/// [`SessionOperative`] with a two-slot, `DropNothing` entry mailbox, so a
/// case can saturate it with two frames and watch a firing meet it.
pub struct SaturatedSessionOperative;
impl StreamOperative for SaturatedSessionOperative {
    fn factory_id() -> &'static str {
        "timers_saturated_session_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .on_full(OverflowPolicy::DropNothing)
            .key_by(|s: &String| s.clone())
            .into::<SessionProc<2>>()
            .sink_named("out");
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Tick {
    pub at: u64,
}

pub struct TickTime;
impl flezha_runner::state::TimeExtractor<Tick> for TickTime {
    fn extract(input: &Tick) -> u64 {
        input.at
    }
}

/// Records the interleaving of `run` and `on_timer` so a case can assert the
/// before-the-input rule rather than infer it.
pub static ORDER: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

pub fn order() -> &'static Mutex<Vec<String>> {
    ORDER.get_or_init(|| Mutex::new(Vec::new()))
}

pub fn order_snapshot() -> Vec<String> {
    order().lock().unwrap_or_else(|p| p.into_inner()).clone()
}

pub fn take_order() -> Vec<String> {
    std::mem::take(&mut *order().lock().unwrap_or_else(|p| p.into_inner()))
}

pub fn note_order(entry: String) {
    order()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .push(entry);
}

/// Event-time fixture: the first input arms three timers ahead of itself; a
/// later input crosses two of them.
#[derive(Default)]
pub struct WindowProc;
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

pub struct WindowOperative;
impl StreamOperative for WindowOperative {
    fn factory_id() -> &'static str {
        "timers_window_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Tick>("in").into::<WindowProc>();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SurfaceCmd {
    Register,
    Cancel,
    RegisterMany(usize),
    RegisterEventTime,
}

#[derive(Default)]
pub struct SurfaceProc;
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

pub fn tick(at: u64) -> Vec<u8> {
    serde_json::to_vec(&Tick { at }).expect("encode tick")
}

pub fn pending(slot: &Arc<flezha_runner::entity::EntitySlot>, kind: TimerKind) -> Vec<u64> {
    slot.timers
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .all(kind)
}

/// Build one controller-framed WAL record carrying timer ops
/// (`format_id = 3`), the shape `init_entity_with_payload` folds.
pub fn timer_ops_record<S: Procedure>(seq: u64, ops: &[TimerOp]) -> Vec<u8> {
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
pub fn effect_record<S: Procedure>(
    seq: u64,
    changes: Vec<Vec<u8>>,
    timers: Vec<TimerOp>,
) -> Vec<u8> {
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

pub fn restore<S: Procedure>(change_log: &[u8]) -> (S::State, PendingTimers)
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
pub type ShippedFrames = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

/// Install a sink that records every state frame the runtime ships.
pub fn recording_sink() -> ShippedFrames {
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

pub fn frame_parts(frame: &[u8]) -> (u64, u8, Vec<u8>) {
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

pub fn frame_format_id(frame: &[u8]) -> u8 {
    frame_parts(frame).1
}

pub fn decode_timer_frame(frame: &[u8]) -> (u64, Vec<TimerOp>) {
    let (seq, _fmt, body) = frame_parts(frame);
    (seq, postcard::from_bytes(&body).expect("decode timer ops"))
}

pub fn effect_bodies(frames: &ShippedFrames) -> Vec<EffectRecordBody> {
    frames
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|(_, f)| frame_format_id(f) == FORMAT_ID_EFFECT_RECORD)
        .map(|(_, f)| postcard::from_bytes(&frame_parts(f).2).expect("decode effect body"))
        .collect()
}
