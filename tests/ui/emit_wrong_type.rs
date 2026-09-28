//! Emits are typed as the tasklet's `Output`.
use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

#[derive(Default)]
struct Echo;

#[tasklet(name = "echo")]
impl Tasklet for Echo {
    type Input = String;
    type Output = String;

    fn run(&mut self, _: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(42u64)
    }
}

fn main() {}
