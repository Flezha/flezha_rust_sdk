//! Sticky per-Entity ordering, with persistent state.
//!
//! The crate's worked example: one entry channel feeding one procedure that
//! appends every input it sees to its own durable state. It is the smallest
//! class that exercises all four halves of the authoring surface — a
//! `Procedure` with `Change`, a `StreamOperative` that wires it, a runner that
//! declares the class, and the state module the mutation is routed through.
//!
//! The invariant it demonstrates: when 10,000 messages targeting one
//! `appender` Entity arrive on the entry channel, `state.seen` ends up holding
//! those 10,000 inputs in arrival order. The per-Entity inflight CAS
//! guarantees no two `Procedure::run` calls overlap on the same Entity, so no
//! ordering work is left to the author.
//!
//! Run it:
//!     Flezha::create_runner("my_namespace")?
//!         .define_stream::<AppendOperative>()?
//!         .run()?; // `let runner = ...start()?;` to keep the calling thread
//!
//! then feed it:
//!     flz operative send appender --channel input --text '"first"'

use flezha_runner::state::{Change, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::{ChannelBuilder, Procedure, ProcedureCtx, ProcedureError, StreamOperative};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default)]
struct AppendState {
    seen: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct AppendChange(String);
impl Change for AppendChange {}

#[derive(Default)]
struct AppendProcedure;

impl Procedure for AppendProcedure {
    type Input = String;
    type Output = String;
    type State = AppendState;
    type Change = AppendChange;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "append_procedure"
    }

    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> AppendState {
        AppendState { seen: vec![] }
    }

    // The fold the runtime replays on restore. Keep it pure: it runs again
    // over the WAL, and a fold that read the clock would rebuild a state the
    // live run never had.
    fn apply_change(state: &mut AppendState, change: &AppendChange) {
        state.seen.push(change.0.clone());
    }

    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        // Every mutation goes through `apply`, never through the state
        // directly — that is what makes it durable and replayable.
        state.apply(AppendChange(input));
        Ok(())
    }
}

struct AppendOperative;

impl StreamOperative for AppendOperative {
    fn factory_id() -> &'static str {
        "appender"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<AppendProcedure>();
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
