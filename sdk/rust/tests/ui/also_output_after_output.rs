// `.also_output_with(...)` chained after `.output(...)` must be a compile
// error. `output` returns a finalized `Channel<T>` handle that exposes no
// further builder methods. This pin guards against a future refactor that
// accidentally relaxes the type-system constraint.

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::operation::{Operation, OperationCtx, OperationError};
use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};

#[derive(Default)]
struct PluginProducer;
impl Operation for PluginProducer {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str { "plugin_producer" }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(&mut self, _: String, _: StateHandle<'_, Self>, _: &mut OperationCtx<'_>) -> Result<(), OperationError> {
        Ok(())
    }
}

fn declare(c: &ChannelBuilder) {
    c.input::<String>("in")
        .into::<PluginProducer>()
        .output("k")
        .also_output_with("k2", |_v: &Vec<u8>| Vec::new());
}

fn main() {}
