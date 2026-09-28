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
use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

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

#[derive(Default)]
struct Downstream;
#[tasklet(name = "downstream")]
impl Tasklet for Downstream {
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    fn run(&mut self, _: Vec<u8>, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
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
