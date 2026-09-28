//! An item the attribute writes may not also be written by the block: the
//! name key sets `NAME`, and the `max_cells` key sets `max_cells`.
use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

#[derive(Default)]
struct Named;

#[tasklet(name = "named")]
impl Tasklet for Named {
    type Input = String;
    const NAME: &'static str = "named";

    fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

#[derive(Default)]
struct Capped;

#[tasklet(name = "capped", max_cells = 10)]
impl Tasklet for Capped {
    type Input = String;

    fn max_cells() -> usize {
        10
    }

    fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

fn main() {}
