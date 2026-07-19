// A stream handle borrows the builder, so it cannot outlive the
// `channels(c)` declaration — the topology freezes when registration
// consumes the builder, and any handle stashed for later use is a
// compile error, not a runtime failure.

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::operation::{Operation, OperationCtx, OperationError};
use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::Stream;

#[derive(Default)]
struct PassOp;
impl Operation for PassOp {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str { "pass_op" }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(&mut self, _: String, _: StateHandle<'_, Self>, _: &mut OperationCtx<'_>) -> Result<(), OperationError> {
        Ok(())
    }
}

static STASH: std::sync::Mutex<Option<Stream<'static, String>>> = std::sync::Mutex::new(None);

fn declare(c: &ChannelBuilder) {
    let stream = c.input::<String>("in").into::<PassOp>();
    *STASH.lock().unwrap() = Some(stream);
}

fn main() {}
