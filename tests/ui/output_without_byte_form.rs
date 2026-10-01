// A Tasklet whose `Output` has no byte form. The output of one tasklet is
// the handoff to the next, and a durable steps journals that handoff at its
// commit barrier, so the type has to be writable to bytes and readable back.
// The refusal must be the SDK's own sentence, naming the type — not serde's
// message about a trait the author never wrote.

use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

#[derive(Clone)]
struct Receipt;

#[derive(Default)]
struct Charge;
#[tasklet(name = "charge")]
impl Tasklet for Charge {
    type Input = String;
    type Output = Receipt;
    fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

fn main() {}
