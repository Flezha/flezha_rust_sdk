//! Dynamic keying lives on the wiring, so the attribute offers no `cell_key`
//! hook and declaring one names its replacement.
use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

#[derive(Default)]
struct P;

#[tasklet(name = "p", cell_key = sk)]
impl Tasklet for P {
    type Input = String;

    fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

fn main() {}
