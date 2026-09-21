// A stream handle borrows the builder, so it cannot outlive the
// `channels(c)` declaration — the topology freezes when registration
// consumes the builder, and any handle stashed for later use is a
// compile error, not a runtime failure.

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::tasklet::{Tasklet, TaskletCtx, TaskletError};
use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::Stream;

#[derive(Default)]
struct PassTask;
impl Tasklet for PassTask {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str { "pass_op" }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(&mut self, _: String, _: StateHandle<'_, Self>, _: &mut TaskletCtx<'_>) -> Result<(), TaskletError> {
        Ok(())
    }
}

static STASH: std::sync::Mutex<Option<Stream<'static, String>>> = std::sync::Mutex::new(None);

fn declare(c: &ChannelBuilder) {
    let stream = c.input::<String>("in").into::<PassTask>();
    *STASH.lock().unwrap() = Some(stream);
}

fn main() {}
