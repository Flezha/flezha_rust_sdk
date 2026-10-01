//! Sticky per-Cell ordering, with persistent state.
//!
//! The crate's worked example: one entry channel feeding one tasklet that
//! appends every input it sees to its own durable state. It is the smallest
//! class that exercises all four parts of the authoring surface: a tasklet
//! with state, the change that records its mutation, an `Operator`
//! that wires it, and a runner that registers the class.
//!
//! The invariant it demonstrates: when 10,000 messages targeting one
//! `appender` Cell arrive on the entry channel, `state.seen` ends up holding
//! those 10,000 inputs in arrival order. The per-Cell inflight CAS
//! guarantees no two `Tasklet::run` calls overlap on the same Cell, so no
//! ordering work is left to the author.
//!
//! Run it:
//!     Flezha::create_runner("my_namespace")?
//!         .register_factory(AppendOperator)?
//!         .run()?; // bind the runner and call `start()` to keep the calling thread
//!
//! then feed it:
//!     flz operator send appender --channel input --text '"first"'

use flezha_runner::{Change, ChannelBuilder, Ctx, Operator, Tasklet, TaskletError, tasklet};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default)]
struct AppendState {
    seen: Vec<String>,
}

/// One input appended. The change applies itself, on the live run and on
/// every restore.
#[derive(Serialize, Deserialize)]
struct Appended(String);

impl Change<AppendState> for Appended {
    fn apply_to(&self, state: &mut AppendState) {
        state.seen.push(self.0.clone());
    }
}

#[derive(Default)]
struct Append;

#[tasklet(name = "append_tasklet")]
impl Tasklet for Append {
    type Input = String;
    type State = AppendState;
    type Change = Appended;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        // Every mutation goes through `ctx.change`, never through the state
        // directly: that is what makes it durable and replayable.
        ctx.change(Appended(input));
        Ok(())
    }
}

struct AppendOperator;

impl Operator for AppendOperator {
    fn factory_id() -> &'static str {
        "appender"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<Append>();
    }
}

fn main() {
    // Declaration-only example: `cargo run --example append_operator` builds
    // the topology and prints its shape rather than dialing a controller.
    match flezha_runner::operator::builder::build_topology_for::<AppendOperator>() {
        Ok(topology) => {
            println!("operator `{}`", topology.factory_id);
            for channel in &topology.channels {
                println!(
                    "  channel `{}` ({:?}) -> {} consumer(s)",
                    channel.name,
                    channel.kind,
                    channel.consumers.len()
                );
            }
        }
        Err(e) => eprintln!("topology refused: {e}"),
    }
}
