// `.on_full(...)` chained after `.sealed()` must be a compile error: a sealed
// channel accepts no further declaration of any kind, and a plugin-bound one
// has no SDK-side mailbox to apply backpressure to anyway (that is wire-level
// — OS socket buffer + transport flow control). `sealed` returns a bare
// `Channel<T>` handle that does not expose `.on_full`, so this fails at the
// call site.

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::OverflowPolicy;

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

fn declare(c: &ChannelBuilder) {
    c.input::<String>("in")
        .into::<PluginProducer>()
        .sink()
        .sealed()
        .on_full(OverflowPolicy::DropNothing);
}

fn main() {}
