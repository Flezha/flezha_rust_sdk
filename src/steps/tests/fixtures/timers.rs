//! Timers inside a steps relay: a loop whose middle tasklet arms them, and a
//! chained loop whose carry must yield to a firing.

use std::sync::Mutex;
use std::time::Duration;

use super::SHORT_SLEEP;
use crate::channel::ChannelBuilder;
use crate::steps::*;
use crate::tasklet::{Ctx, Tasklet, TaskletError};
use crate::{Durable, KeepFinished, Relay, RelaySettings, Run, SleepAfter};

/// Every timestamp a fixture here armed, in registration order.
pub(crate) static ARMED: Mutex<Vec<u64>> = Mutex::new(Vec::new());

pub(crate) fn drain_armed() -> Vec<u64> {
    std::mem::take(&mut *ARMED.lock().unwrap())
}

fn arm<S: Tasklet>(ctx: &mut Ctx<'_, S>, after: Duration) -> Result<u64, TaskletError> {
    let at = ctx.timer_after(after)?;
    ARMED.lock().unwrap().push(at);
    Ok(at)
}

fn millis(text: &str) -> Duration {
    Duration::from_millis(text.parse().unwrap_or(0))
}

string_task!(Front, "timer_front", |input, ctx| ctx.emit(input));

/// `after:<ms>` arms a timer and emits `armed:<at>`; `after-slow:<ms>` the
/// same, marked for the tock to linger on; `hold:<ms>` holds the stage that
/// long. A firing emits `fired:<at>`.
#[derive(Default)]
pub(crate) struct Tick;
#[flezha_runner::tasklet(name = "timer_tick")]
impl Tasklet for Tick {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
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
        firing: crate::cell::timer::TimerFiring,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        ctx.emit(format!("fired:{}", firing.scheduled_at))
    }
}

string_task!(Tock, "timer_tock", |input, ctx| {
    if input.starts_with("armed-slow") {
        std::thread::sleep(Duration::from_millis(150));
    }
    ctx.emit(format!("tock:{input}"))
});

/// `in -> front -> tick -> tock -> out`: `tick`, which sets the timers, is
/// not the entry channel's root.
pub(crate) struct TimerLoop;
impl Relay for TimerLoop {
    fn factory_id() -> &'static str {
        "test-timer-loop"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<Front>()
            .named("fronted")
            .into::<Tick>()
            .named("ticked")
            .into::<Tock>()
            .output_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_secs(30)),
            durable: Durable::Local {
                sleep_after: SleepAfter::After(SHORT_SLEEP),
                keep_finished: KeepFinished::Nothing,
            },
            ..RelaySettings::default()
        }
    }
}

/// Arms a timer 20 ms out on its first message and passes every input on;
/// its firing releases nothing.
#[derive(Default)]
pub(crate) struct ArmOnce;
#[flezha_runner::tasklet(name = "timer_arm_once")]
impl Tasklet for ArmOnce {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        if input == "3" {
            arm(ctx, Duration::from_millis(20))?;
        }
        ctx.emit(input)
    }
    fn on_timer(
        &mut self,
        _firing: crate::cell::timer::TimerFiring,
        _ctx: &mut Ctx<'_, Self>,
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
impl Relay for ChainedTimerLoop {
    fn factory_id() -> &'static str {
        "test-chained-timer-loop"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ArmOnce>()
            .named("armed")
            .into::<TimerCountdown>()
            .named("carry");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Chained,
                on_failure: OnFailure::UndoAll,
            },
            ..RelaySettings::default()
        }
    }
}
