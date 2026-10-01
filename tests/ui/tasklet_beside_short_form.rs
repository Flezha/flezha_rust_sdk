//! A short-form attribute already completes the block and names its tasklet,
//! so `#[flezha::tasklet]` on the same block is refused.
use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

#[derive(Default)]
struct Counter;

#[flezha_runner::operator(id = "counter")]
#[tasklet(name = "counter")]
impl Tasklet for Counter {
    type Input = String;

    fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

fn main() {}
