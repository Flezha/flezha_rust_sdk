//! Per-Entity timers: a session that closes itself when it goes quiet.
//!
//! Run with `cargo run --example session_timeout` (needs a controller to
//! connect to; the point of the file is the authoring shape).
//!
//! The whole reason a timer exists is the last line of `run`: without it,
//! nothing would ever close a session whose user simply stopped clicking.
//! There is no input to carry that decision — only the passage of time.

use std::time::Duration;

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::operative::StreamOperative;
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError, StateHandle};
use flezha_runner::state::{Change, RestoreInitPayload, TimerKind, WallClock};
use flezha_runner::Flezha;
use serde::{Deserialize, Serialize};

/// How long a session may stay quiet before it is closed.
const IDLE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SessionChange {
    Clicked(String),
    Closed { at: u64, clicks: usize },
}
impl Change for SessionChange {}

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
    /// Per-Entity scratch: the runtime holds one Procedure instance per Entity.
    deadline: Option<u64>,
}

impl Procedure for SessionWindow {
    type Input = String;
    type Output = ClosedSession;
    type State = SessionState;
    type Change = SessionChange;
    // A WALL clock, deliberately. `timer_after` is anchored on `ctx.now()`, and
    // the deadline this Procedure wants is "30s of real silence".
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "session_window"
    }

    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> SessionState {
        SessionState::default()
    }

    fn apply_change(state: &mut SessionState, change: &SessionChange) {
        match change {
            SessionChange::Clicked(page) => state.clicks.push(page.clone()),
            SessionChange::Closed { .. } => state.closed = true,
        }
    }

    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(SessionChange::Clicked(input));

        // Retract the deadline this click supersedes, then set the new one.
        // Both are durable ops on the pending set, so a crash between them is
        // not a state a restore can land in.
        if let Some(previous) = self.deadline.take() {
            ctx.cancel_processing_time_timer(previous)?;
        }
        self.deadline = Some(ctx.timer_after(IDLE)?);
        Ok(())
    }

    fn on_timer(
        &mut self,
        fired_at: u64,
        _kind: TimerKind,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        self.deadline = None;
        let clicks = state.clicks.len();
        // `fired_at` is the SCHEDULED instant, not the wall clock at firing —
        // so this record is identical on a replay.
        state.apply(SessionChange::Closed {
            at: fired_at,
            clicks,
        });
        ctx.emit(ClosedSession {
            user: String::new(),
            clicks,
            closed_at: fired_at,
        })
    }
}

struct SessionOperative;

impl StreamOperative for SessionOperative {
    fn factory_id() -> &'static str {
        "session_window"
    }

    fn channels(c: &ChannelBuilder) {
        // One Entity per user: the key is declared on the WIRING, and each
        // Entity owns its own idle deadline.
        c.input::<String>("clicks")
            .key_by(|click: &String| click.split(':').next().unwrap_or(click).to_string())
            .into::<SessionWindow>()
            .named("closed_sessions")
            .output("kafka");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    Flezha::create_runner("sessions")?
        .define_stream::<SessionOperative>()?
        .run()?;
    Ok(())
}
