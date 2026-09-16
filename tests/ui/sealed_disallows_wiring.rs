// `.sealed()` is the author's statement that nothing follows the channel, so
// wiring a consumer off the handle it returns must be a compile error:
// `sealed` yields a finalized `Channel<T>` exposing no builder methods.
//
// The diagnostic is E0107, not "method not found": every type picks up
// `Into::into` from the std blanket impl, so `.into::<Next>()` resolves to it
// and rustc complains about the turbofish instead. Still a refusal at the
// offending call site — see `sealed_disallows_on_full.rs` for the clean
// "method not found" shape on a name std does not claim.

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};

#[derive(Default)]
struct PluginProducer;
impl Procedure for PluginProducer {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str { "plugin_producer" }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(&mut self, _: String, _: StateHandle<'_, Self>, _: &mut ProcedureCtx<'_>) -> Result<(), ProcedureError> {
        Ok(())
    }
}

#[derive(Default)]
struct Downstream;
impl Procedure for Downstream {
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str { "downstream" }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(&mut self, _: Vec<u8>, _: StateHandle<'_, Self>, _: &mut ProcedureCtx<'_>) -> Result<(), ProcedureError> {
        Ok(())
    }
}

fn declare(c: &ChannelBuilder) {
    c.input::<String>("in")
        .into::<PluginProducer>()
        .sink()
        .sealed()
        .into::<Downstream>();
}

fn main() {}
