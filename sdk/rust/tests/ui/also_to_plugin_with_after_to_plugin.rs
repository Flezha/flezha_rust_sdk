// `.also_to_plugin_with(...)` chained after `.to_plugin(...)` must be a
// compile error. `to_plugin` returns a finalized `Channel<T>` handle that
// does not expose `also_to_plugin_with`. This pin guards against a
// future refactor that accidentally relaxes the type-system constraint.

use flezha_executor::channel::ChannelBuilder;
use flezha_executor::shard::{Shard, ShardCtx, ShardError};
use flezha_executor::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};

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
        .also_to_plugin_with("k2", |_v: &Vec<u8>| Vec::new());
}

fn main() {}
