//! Sticky per-Entity ordering, with persistent state.
//!
//! The crate's worked example: one entry channel feeding one procedure that
//! appends every input it sees to its own durable state. It is the smallest
//! class that exercises all four halves of the authoring surface: a procedure
//! with state, a `StreamOperative` that wires it, a runner that registers the
//! class, and the derived state whose recorder journals the mutation.
//!
//! The invariant it demonstrates: when 10,000 messages targeting one
//! `appender` Entity arrive on the entry channel, `state.seen` ends up holding
//! those 10,000 inputs in arrival order. The per-Entity inflight CAS
//! guarantees no two `Procedure::run` calls overlap on the same Entity, so no
//! ordering work is left to the author.
//!
//! Run it:
//!     Flezha::create_runner("my_namespace")?
//!         .register_factory(AppendOperative)?
//!         .run()?; // bind the runner and call `start()` to keep the calling thread
//!
//! then feed it:
//!     flz operative send appender --channel input --text '"first"'

use flezha_runner::{procedure, ChannelBuilder, StreamOperative};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default, flezha_runner::State)]
struct AppendState {
    seen: Vec<String>,
}

#[procedure(id = "append_procedure")]
fn append(input: String, state: &mut AppendState) {
    // Every mutation goes through a generated recorder, never through the
    // state directly: that is what makes it durable and replayable.
    state.push_seen(input);
}

struct AppendOperative;

impl StreamOperative for AppendOperative {
    fn factory_id() -> &'static str {
        "appender"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<Append>();
    }
}

fn main() {
    // Declaration-only example: `cargo run --example append_operative` builds
    // the topology and prints its shape rather than dialing a controller.
    match flezha_runner::operative::builder::build_topology_for::<AppendOperative>() {
        Ok(topology) => {
            println!("operative `{}`", topology.factory_id);
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
