// The simplified tier IS a `Tasklet` (blanket impl), so declaring both for
// one type is two implementations of one trait. Coherence refuses it — which
// is what makes "explicit always wins" unambiguous rather than a precedence
// rule someone has to remember.
use flezha_runner::tasklet::{
    NoChange, Tasklet, TaskletCtx, TaskletError, SimpleTasklet, StateHandle, WallClock,
};
use flezha_runner::RestoreInitPayload;

#[derive(Default)]
struct Both;

impl SimpleTasklet for Both {
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
        _ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        Ok(())
    }
}

impl Tasklet for Both {
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
        _ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        Ok(())
    }
}

fn main() {}
