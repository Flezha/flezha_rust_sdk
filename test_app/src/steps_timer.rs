//! Steps fixtures fed by timers: a per-input loop whose tasklet arms
//! processing-time timers, and one that arms event-time timers.

use crate::steps::{block_until_released, stateless_step};
use crate::*;

// `timer_loop_steps`: `in -> tick -> tock -> out`, a per-input loop whose
// `tick` arms a processing-time timer of the delay `after:<ms>` names and
// emits `armed:<at>`; its firing emits `fired:<at>` and opens a pass of its
// own. `slow:<ms>` holds `tick` that long, for a firing due mid-pass. On a
// runner booted with `FLEZHA_STEPS_E2E_HOLD` the firing parks at the gate:
// `timer` inside `on_timer`, before `tick` commits; `tock` in `tock`, after.
const E2E_HOLD_ENV: &str = "FLEZHA_STEPS_E2E_HOLD";

fn hold_at(point: &str) {
    if env::var(E2E_HOLD_ENV).is_ok_and(|held| held == point) {
        block_until_released();
    }
}

#[derive(Default)]
struct Tick;
#[flezha_runner::tasklet(name = "tick")]
impl Tasklet for Tick {
    type Input = String;
    type Output = String;
    type State = SeenState;
    type Change = Insert;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let millis = |text: &str| Duration::from_millis(text.parse().unwrap_or(0));
        if let Some(delay) = input.strip_prefix("after:") {
            let at = ctx.timer_after(millis(delay))?;
            ctx.change(Insert(format!("armed:{at}")));
            return ctx.emit(format!("armed:{at}"));
        }
        if let Some(pause) = input.strip_prefix("slow:") {
            thread::sleep(millis(pause));
        }
        ctx.emit(input)
    }
    fn on_timer(
        &mut self,
        fired_at: u64,
        _kind: flezha_runner::state::TimerKind,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        hold_at("timer");
        ctx.emit(format!("fired:{fired_at}"))
    }
}

stateless_step!(Tock, "tock", |input, ctx| {
    if input.starts_with("fired:") {
        hold_at("tock");
    }
    ctx.emit(format!("tock:{input}"))
});

pub(crate) struct TimerLoopSteps;
impl Relay for TimerLoopSteps {
    fn factory_id() -> &'static str {
        "timer_loop_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<Tick>()
            .named("ticked")
            .into::<Tock>()
            .sink_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_secs(600)),
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// `event_timer_steps`: an entry-fed tasklet whose clock is the message's
// leading event time; each message arms an event-time timer 100 later and
// emits `seen:<t>`, and the firing emits `fired:<at>`.
#[derive(Default)]
struct EventTick;
#[flezha_runner::tasklet(name = "event_tick")]
impl Tasklet for EventTick {
    type Input = String;
    type Output = String;
    type State = SeenState;
    type Change = Insert;
    fn event_time(input: &String) -> u64 {
        trade_event_time(input)
    }
    fn run(&mut self, _input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let at = ctx.timer_at_event_time(ctx.now() + 100)?;
        ctx.change(Insert(format!("armed:{at}")));
        ctx.emit(format!("seen:{}", ctx.now()))
    }
    fn on_timer(
        &mut self,
        fired_at: u64,
        _kind: flezha_runner::state::TimerKind,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        ctx.emit(format!("fired:{fired_at}"))
    }
}

pub(crate) struct EventTimerSteps;
impl Relay for EventTimerSteps {
    fn factory_id() -> &'static str {
        "event_timer_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<EventTick>()
            .sink_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_secs(600)),
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}
