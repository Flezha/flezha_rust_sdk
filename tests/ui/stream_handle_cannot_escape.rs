// A stream handle borrows the builder, so it cannot outlive the
// `channels(c)` declaration — the topology freezes when registration
// consumes the builder, and any handle stashed for later use is a
// compile error, not a runtime failure.

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};
use flezha_runner::Stream;

#[derive(Default)]
struct PassTask;
#[tasklet(name = "pass_op")]
impl Tasklet for PassTask {
    type Input = String;
    type Output = String;
    fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

static STASH: std::sync::Mutex<Option<Stream<'static, String>>> = std::sync::Mutex::new(None);

fn declare(c: &ChannelBuilder) {
    let stream = c.input::<String>("in").into::<PassTask>();
    *STASH.lock().unwrap() = Some(stream);
}

fn main() {}
