//! Per-Cell timers: a session that closes itself when it goes quiet.
//!
//! Run with `cargo run --example session_timeout` (needs a controller to
//! connect to; the point of the file is the authoring shape).
//!
//! The whole reason a timer exists is the last line of `run`: without it,
//! nothing would ever close a session whose user simply stopped clicking.
//! There is no input to carry that decision, only the passage of time.

use std::time::Duration;

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::relay::Relay;
use flezha_runner::state::{Change, TimerFiring, TimerKind};
use flezha_runner::tasklet::{Ctx, Tasklet, TaskletError};
use flezha_runner::Flezha;
use serde::{Deserialize, Serialize};

/// How long a session may stay quiet before it is closed.
const IDLE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SessionChange {
    Clicked(String),
    Closed { at: u64, clicks: usize },
}
impl Change<SessionState> for SessionChange {
    fn apply_to(&self, state: &mut SessionState) {
        match self {
            SessionChange::Clicked(page) => state.clicks.push(page.clone()),
            SessionChange::Closed { .. } => state.closed = true,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct SessionState {
    clicks: Vec<String>,
    closed: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClosedSession {
    user: String,
    clicks: usize,
    closed_at: u64,
}

#[derive(Default)]
struct SessionWindow {
    /// The deadline currently pending, so each click can retract the last one.
    /// Per-Cell scratch: the runtime holds one Tasklet instance per Cell.
    deadline: Option<u64>,
}

#[flezha_runner::tasklet(name = "session_window")]
impl Tasklet for SessionWindow {
    type Input = String;
    type Output = ClosedSession;
    type State = SessionState;
    type Change = SessionChange;

    // No `event_time`, so the clock is the wall clock, deliberately:
    // `timer_after` is anchored on `ctx.now()`, and the deadline this tasklet
    // wants is "30s of real silence".
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(SessionChange::Clicked(input));

        // Retract the deadline this click supersedes, then set the new one.
        // Both are durable ops on the pending set, so a crash between them is
        // not a state a restore can land in.
        if let Some(previous) = self.deadline.take() {
            ctx.cancel_timer(TimerKind::ProcessingTime, previous)?;
        }
        self.deadline = Some(ctx.timer_after(IDLE)?);
        Ok(())
    }

    fn on_timer(
        &mut self,
        firing: TimerFiring,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        self.deadline = None;
        let clicks = ctx.state().clicks.len();
        // `firing.scheduled_at` is the SCHEDULED instant, not the wall clock at
        // firing, so this record is identical on a replay.
        ctx.change(SessionChange::Closed {
            at: firing.scheduled_at,
            clicks,
        });
        ctx.emit(ClosedSession {
            user: String::new(),
            clicks,
            closed_at: firing.scheduled_at,
        })
    }
}

struct SessionRelay;

impl Relay for SessionRelay {
    fn factory_id() -> &'static str {
        "session_window"
    }

    fn channels(c: &ChannelBuilder) {
        // One Cell per user: the key is declared on the WIRING, and each
        // Cell owns its own idle deadline.
        c.input::<String>("clicks")
            .key_by(|click: &String| click.split(':').next().unwrap_or(click).to_string())
            .into::<SessionWindow>()
            .output_named("closed_sessions");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    Flezha::create_runner("sessions")?
        .register_factory(SessionRelay)?
        .run()?;
    Ok(())
}
