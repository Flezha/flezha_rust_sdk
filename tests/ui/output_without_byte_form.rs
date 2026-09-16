// A Procedure whose `Output` has no byte form. The output of one procedure is
// the handoff to the next, and a durable workflow journals that handoff at its
// commit barrier, so the type has to be writable to bytes and readable back.
// The refusal must be the SDK's own sentence, naming the type — not serde's
// message about a trait the author never wrote.

use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};

#[derive(Clone)]
struct Receipt;

#[derive(Default)]
struct Charge;
impl Procedure for Charge {
    type Input = String;
    type Output = Receipt;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str { "charge" }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(&mut self, _: String, _: StateHandle<'_, Self>, _: &mut ProcedureCtx<'_>) -> Result<(), ProcedureError> {
        Ok(())
    }
}

fn main() {}
