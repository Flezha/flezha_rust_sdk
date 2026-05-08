// `.on_full(...)` chained after `.to_plugin(...)` must be a compile error
// because plugin-bound channels have no SDK-side mailbox; backpressure is
// wire-level (OS socket buffer + transport flow control). The
// `ChannelDeclWithProducer::to_plugin` consumer returns a bare
// `Channel<T>` handle that does not expose `.on_full`, so this fails at
// the call site.

use flezha_executor::channel::ChannelBuilder;
use flezha_executor::shard::{Shard, ShardCtx, ShardError};
use flezha_executor::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_executor::OverflowPolicy;

#[derive(Default)]
struct PluginProducer;
impl Shard for PluginProducer {
    type Input = ();
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str { "plugin_producer" }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(&mut self, _: (), _: StateHandle<'_, Self>, _: &mut ShardCtx<'_>) -> Result<(), ShardError> {
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
