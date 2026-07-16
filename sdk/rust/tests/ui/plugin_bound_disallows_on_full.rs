// `.on_full(...)` chained after `.to_plugin(...)` must be a compile error
// because plugin-bound channels have no SDK-side mailbox; backpressure is
// wire-level (OS socket buffer + transport flow control). The
// `ChannelDeclWithProducer::to_plugin` consumer returns a bare
// `Channel<T>` handle that does not expose `.on_full`, so this fails at
// the call site.

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::operation::{Operation, OperationCtx, OperationError};
use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::OverflowPolicy;

#[derive(Default)]
struct PluginProducer;
impl Operation for PluginProducer {
    type Input = ();
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str { "plugin_producer" }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(&mut self, _: (), _: StateHandle<'_, Self>, _: &mut OperationCtx<'_>) -> Result<(), OperationError> {
        Ok(())
    }
}

fn declare(c: &mut ChannelBuilder) {
    c.channel::<Vec<u8>>("p")
        .from::<PluginProducer>()
        .to_plugin("k")
        .on_full(OverflowPolicy::Block);
}

fn main() {}
