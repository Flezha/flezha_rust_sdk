//! Timers inside a bounded operator: a loop whose middle tasklet arms them, and a
//! chained loop whose carry must yield to a firing.

use std::sync::Mutex;
use std::time::Duration;

use crate::bounded::*;
use crate::cell::restore::RestoreInitPayload;
use crate::channel::ChannelBuilder;
use crate::tasklet::change::NoChange;
use crate::tasklet::clock::WallClock;
use crate::tasklet::{StateHandle, Tasklet, TaskletCtx, TaskletError};

/// Every timestamp a fixture here armed, in registration order.
pub(crate) static ARMED: Mutex<Vec<u64>> = Mutex::new(Vec::new());

pub(crate) fn drain_armed() -> Vec<u64> {
    std::mem::take(&mut *ARMED.lock().unwrap())
}

fn arm(ctx: &mut TaskletCtx<'_>, after: Duration) -> Result<u64, TaskletError> {
    let at = ctx.timer_after(after)?;
    ARMED.lock().unwrap().push(at);
    Ok(at)
}

fn millis(text: &str) -> Duration {
    Duration::from_millis(text.parse().unwrap_or(0))
}

string_task!(Front, "timer_front", |input, ctx| ctx.emit(input));

/// `after:<ms>` arms a timer and emits `armed:<at>`; `after-slow:<ms>` the
/// same, marked for the relay to linger on; `hold:<ms>` holds the stage that
/// long. A firing emits `fired:<at>`.
#[derive(Default)]
pub(crate) struct Tick;
impl Tasklet for Tick {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "timer_tick"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        if let Some(hold) = input.strip_prefix("hold:") {
            std::thread::sleep(millis(hold));
        }
        if let Some(after) = input.strip_prefix("after-slow:") {
            let at = arm(ctx, millis(after))?;
            return ctx.emit(format!("armed-slow:{at}"));
        }
        if let Some(after) = input.strip_prefix("after:") {
            let at = arm(ctx, millis(after))?;
            return ctx.emit(format!("armed:{at}"));
        }
        ctx.emit(input)
    }
    fn on_timer(
        &mut self,
        fired_at: u64,
        _kind: crate::cell::timer::TimerKind,
        _state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        ctx.emit(format!("fired:{fired_at}"))
    }
}

string_task!(Relay, "timer_relay", |input, ctx| {
    if input.starts_with("armed-slow") {
        std::thread::sleep(Duration::from_millis(150));
    }
    ctx.emit(format!("relay:{input}"))
});

/// `in -> front -> tick -> relay -> out`: `tick`, which sets the timers, is
/// not the entry channel's root.
pub(crate) struct TimerLoop;
impl BoundedOperator for TimerLoop {
    fn factory_id() -> &'static str {
        "test-timer-loop"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn self_terminating() -> bool {
        false
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_secs(30))
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<Front>()
            .named("fronted")
            .into::<Tick>()
            .named("ticked")
            .into::<Relay>()
            .sink_named("out");
    }
}

/// Arms a timer 20 ms out on its first message and passes every input on;
/// its firing releases nothing.
#[derive(Default)]
pub(crate) struct ArmOnce;
impl Tasklet for ArmOnce {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "timer_arm_once"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        if input == "3" {
            arm(ctx, Duration::from_millis(20))?;
        }
        ctx.emit(input)
    }
    fn on_timer(
        &mut self,
        _fired_at: u64,
        _kind: crate::cell::timer::TimerKind,
        _state: StateHandle<'_, Self>,
        _ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        Ok(())
    }
}

string_task!(TimerCountdown, "timer_countdown", |input, ctx| {
    match input.parse::<u32>().unwrap_or(0) {
        0 => Ok(()),
        n => ctx.emit((n - 1).to_string()),
    }
});

/// A durable chained loop `in -> arm -> countdown`, the countdown's own
/// channel the carry.
pub(crate) struct ChainedTimerLoop;
impl BoundedOperator for ChainedTimerLoop {
    fn factory_id() -> &'static str {
        "test-chained-timer-loop"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::Chained
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ArmOnce>()
            .named("armed")
            .into::<TimerCountdown>()
            .named("carry");
    }
}
