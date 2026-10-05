//! A two-tasklet graph run in this process, with no controller and no
//! Docker.
//!
//! Run it with:
//!     cargo run -p flezha_runner --example local_word_count
//!
//! State is held in memory and NOTHING is durable: the run says so on
//! stderr before it does any work. Durability, plugins, placement and
//! migration, the dead-letter queue, observability export, authz and
//! multi-runner scale all need a controller. `docs/quickstart.md` walks
//! through the same graph.
//!
//! What runs here is the deployed machinery, not an imitation: the entry
//! channel's decode, the edge's `flat_map` and `key_by`, one Cell per
//! word with its own mailbox, and the inter-tasklet hop from `Split` to
//! `Tally` performed by the runtime, and `.output_named("totals")` is what the
//! output callback registers against: an output names the channel it terminates,
//! and the no-name form leaves it the producing tasklet's. `WordCount` is an
//! ordinary relay class. Hand the same type to
//! `Flezha::create_runner(..).register_factory(..)` and it deploys unchanged.

use std::time::Duration;

use flezha_runner::{
    tasklet, Change, ChannelBuilder, Ctx, LocalRelay, Relay, Tasklet, TaskletError,
};
use serde::{Deserialize, Serialize};

/// One line into its words. Stateless: the whole point of the first
/// tasklet is that the hop to the second one is the runtime's job.
#[derive(Default)]
struct Split;

#[tasklet(name = "split")]
impl Tasklet for Split {
    type Input = String;
    type Output = String;

    fn run(&mut self, line: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        for word in line.split_whitespace() {
            ctx.emit(word.to_lowercase())?;
        }
        Ok(())
    }
}

/// The count of the word this Cell is.
#[derive(Default, Serialize, Deserialize)]
struct Count {
    seen: u64,
}

/// One more sighting of the word.
#[derive(Serialize, Deserialize)]
struct Seen;

impl Change<Count> for Seen {
    fn apply_to(&self, count: &mut Count) {
        count.seen += 1;
    }
}

/// One Cell per word, minted on first sight of it.
#[derive(Default)]
struct Tally;

#[tasklet(name = "tally")]
impl Tasklet for Tally {
    type Input = String;
    type Output = String;
    type State = Count;
    type Change = Seen;

    fn run(&mut self, word: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Seen);
        ctx.emit(format!("{word} {}", ctx.state().seen))
    }
}

struct WordCount;

impl Relay for WordCount {
    fn factory_id() -> &'static str {
        "local_word_count"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("lines")
            .into::<Split>()
            .key_by(|word: &String| word.clone())
            .into::<Tally>()
            .keyed()
            .output_named("totals");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let run = LocalRelay::start::<WordCount>()?;
    run.on_output("totals", |output| {
        println!("totals <- {}", String::from_utf8_lossy(&output.payload));
    })?;

    for line in ["the quick brown fox", "the lazy dog"] {
        run.feed("lines", line)?;
    }
    run.run_until_idle(Duration::from_secs(10))?;
    Ok(())
}
