//! A static cell needs both its keys and the `cells_for` that routes to them.
use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

#[derive(Default)]
struct F;

#[tasklet(name = "f", keys = ["a", "b"])]
impl Tasklet for F {
    type Input = String;

    fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

fn main() {}
