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
//! `Flezha::create_runner(..).register_factory(..)` and it deploys unchanged.

use std::time::Duration;

use flezha_runner::{procedure, ChannelBuilder, LocalOperative, ProcedureError, StreamOperative};
use serde::{Deserialize, Serialize};

/// One line into its words. Stateless: the whole point of the first
/// procedure is that the hop to the second one is the runtime's job.
#[procedure(id = "split", emits = String)]
fn split(line: String, ctx: &mut flezha_runner::ProcedureCtx<'_>) -> Result<(), ProcedureError> {
    for word in line.split_whitespace() {
        ctx.emit(word.to_lowercase())?;
    }
    Ok(())
}

/// The count of the word this Entity is.
#[derive(Default, Serialize, Deserialize, flezha_runner::State)]
struct Count {
    seen: u64,
}

/// One Entity per word, minted on first sight of it.
#[procedure(id = "tally")]
fn tally(word: String, count: &mut Count) -> String {
    count.set_seen(count.seen + 1);
    format!("{word} {}", count.seen)
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
