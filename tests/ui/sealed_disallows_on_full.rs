// `.on_full(...)` chained after `.sealed()` must be a compile error: a sealed
// channel accepts no further declaration of any kind, and a plugin-bound one
// has no SDK-side mailbox to apply backpressure to anyway (that is wire-level
// — OS socket buffer + transport flow control). `sealed` returns a bare
// `Channel<T>` handle that does not expose `.on_full`, so this fails at the
// call site.

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};
use flezha_runner::OverflowPolicy;

#[derive(Default)]
struct PluginProducer;
#[tasklet(name = "plugin_producer")]
impl Tasklet for PluginProducer {
    type Input = String;
    type Output = Vec<u8>;
    fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
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
