//! The context hands out no mutable state: a change the runtime did not
//! record cannot compile.
use flezha_runner::{tasklet, Change, Ctx, Tasklet, TaskletError};

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Count {
    seen: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Seen;

impl Change<Count> for Seen {
    fn apply_to(&self, state: &mut Count) {
        state.seen += 1;
    }
}

#[derive(Default)]
struct Counter;

#[tasklet(name = "counter")]
impl Tasklet for Counter {
    type Input = String;
    type State = Count;
    type Change = Seen;

    fn run(&mut self, _: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.state().seen = 1;
        Ok(())
    }
}

fn main() {}
