//! A tasklet has one clock: `logical_clock` and a written `event_time` are two.
use flezha_runner::{tasklet, Change, Ctx, Tasklet, TaskletError};

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Count(u64);

#[derive(serde::Serialize, serde::Deserialize)]
struct Bump;

impl Change<Count> for Bump {
    fn apply_to(&self, state: &mut Count) {
        state.0 += 1;
    }
}

#[derive(Default)]
struct Seq;

#[tasklet(name = "seq", logical_clock)]
impl Tasklet for Seq {
    type Input = u64;
    type State = Count;
    type Change = Bump;

    fn event_time(input: &u64) -> u64 {
        *input
    }

    fn run(&mut self, _: u64, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Bump);
        Ok(())
    }
}

fn main() {}
