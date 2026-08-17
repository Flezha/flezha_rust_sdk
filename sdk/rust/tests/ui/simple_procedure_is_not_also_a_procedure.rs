// The simplified tier IS a `Procedure` (blanket impl), so declaring both for
// one type is two implementations of one trait. Coherence refuses it — which
// is what makes "explicit always wins" unambiguous rather than a precedence
// rule someone has to remember.
use flezha_runner::procedure::{
    NoChange, Procedure, ProcedureCtx, ProcedureError, SimpleProcedure, StateHandle, WallClock,
};
use flezha_runner::RestoreInitPayload;

#[derive(Default)]
struct Both;

impl SimpleProcedure for Both {
    type Input = u64;
    type Output = u64;
    type State = ();

    fn name() -> &'static str {
        "both"
    }

    fn init_state(_: Option<&str>) {}

    fn run(
        &mut self,
        _input: u64,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Ok(())
    }
}

impl Procedure for Both {
    type Input = u64;
    type Output = u64;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "both"
    }

    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}

    fn run(
        &mut self,
        _input: u64,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Ok(())
    }
}

fn main() {}
