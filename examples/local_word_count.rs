//! A two-procedure graph run in this process, with no controller and no
//! Docker (`sdk-local-mode`).
//!
//! Run it with:
//!     cargo run -p flezha_runner --example local_word_count
//!
//! State is held in memory and NOTHING is durable — the run says so on
//! stderr before it does any work. Durability, plugins, placement and
//! migration, the dead-letter queue, observability export, authz and
//! multi-runner scale all need a controller. `docs/quickstart.md` walks
//! through the same graph.
//!
//! What runs here is the deployed machinery, not an imitation: the entry
//! channel's decode, the edge's `flat_map` and `key_by`, one Entity per
//! word with its own mailbox, and the inter-procedure hop from `Split` to
//! `Tally` performed by the runtime, and `.sink_named("totals")` is what the
//! sink callback registers against — a sink names the channel it terminates,
//! and the no-name form leaves it the producing procedure's. `WordCount` is an
//! ordinary operative
//! class — hand the same type to
//! `Flezha::create_runner(..).define_stream()` and it deploys unchanged.

use std::time::Duration;

use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{Change, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::{ChannelBuilder, LocalOperative, StreamOperative};
use serde::{Deserialize, Serialize};

/// One line into its words. Stateless: the whole point of the first
/// procedure is that the hop to the second one is the runtime's job.
#[derive(Default)]
struct Split;

impl Procedure for Split {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = flezha_runner::NoChange;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "split"
    }

    fn init_state(_key: Option<&str>, _payload: Option<RestoreInitPayload<'_>>) {}

    fn apply_change(_state: &mut (), _change: &flezha_runner::NoChange) {}

    fn run(
        &mut self,
        line: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        for word in line.split_whitespace() {
            ctx.emit(word.to_lowercase())?;
        }
        Ok(())
    }
}

/// One occurrence of the word this Entity is.
#[derive(Clone, Serialize, Deserialize)]
struct Seen;

impl Change for Seen {}

#[derive(Default, Serialize, Deserialize)]
struct Count(u64);

/// One Entity per word, minted on first sight of it.
#[derive(Default)]
struct Tally;

impl Procedure for Tally {
    type Input = String;
    type Output = String;
    type State = Count;
    type Change = Seen;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "tally"
    }

    fn init_state(_key: Option<&str>, _payload: Option<RestoreInitPayload<'_>>) -> Count {
        Count(0)
    }

    fn apply_change(state: &mut Count, _change: &Seen) {
        state.0 += 1;
    }

    fn run(
        &mut self,
        word: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(Seen);
        ctx.emit(format!("{word} {}", state.0))
    }
}

struct WordCount;

impl StreamOperative for WordCount {
    fn factory_id() -> &'static str {
        "local_word_count"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("lines")
            .into::<Split>()
            .key_by(|word: &String| word.clone())
            .into::<Tally>()
            .keyed()
            .sink_named("totals");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let run = LocalOperative::start::<WordCount>()?;
    run.on_sink("totals", |output| {
        println!("totals <- {}", String::from_utf8_lossy(&output.payload));
    })?;

    for line in ["the quick brown fox", "the lazy dog"] {
        run.feed("lines", line)?;
    }
    run.run_until_idle(Duration::from_secs(10))?;
    Ok(())
}
