//! Part fixtures (`parts`): for an input `n` the tasklet sends the parts `1`
//! .. `n` and then emits `done:<n>` on its sink `answer`, in an unbounded
//! operator and in a `per_input` bounded one. `retry:<n>` sends its parts and
//! then fails retryably the first time this process sees it, so the retry's
//! parts number from 0 again under the next attempt.

use std::collections::BTreeSet;
use std::sync::Mutex;

use crate::bounded::stateless_step;
use crate::*;

/// The `retry:` inputs that have already failed once in this process.
static FAILED_ONCE: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());

fn send_parts<S: Tasklet<Output = String>>(
    input: String,
    ctx: &mut Ctx<'_, S>,
) -> Result<(), TaskletError> {
    let count = input.strip_prefix("retry:").unwrap_or(&input);
    let n: u32 = count
        .parse()
        .map_err(|_| TaskletError::non_retryable(format!("not a part count: {input}")))?;
    for part in 1..=n {
        ctx.part(part.to_string())?;
    }
    let first_failure = input.starts_with("retry:")
        && FAILED_ONCE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(input.clone());
    if first_failure {
        return Err(TaskletError::Custom(format!("first attempt of {input}")));
    }
    ctx.emit(format!("done:{n}"))
}

stateless_step!(PartEcho, "part_echo", |input, ctx| send_parts(input, ctx));
stateless_step!(PartEchoTurn, "part_echo_turn", |input, ctx| send_parts(
    input, ctx
));

pub(crate) struct PartEchoOperator;
impl UnboundedOperator for PartEchoOperator {
    fn factory_id() -> &'static str {
        "part_echo"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .retry_backoff_ms(25)
            .into::<PartEcho>()
            .sink_named("answer");
    }
}

pub(crate) struct PartEchoBounded;
impl BoundedOperator for PartEchoBounded {
    fn factory_id() -> &'static str {
        "part_echo_bounded"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_secs(600))
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .retry_backoff_ms(25)
            .into::<PartEchoTurn>()
            .sink_named("answer");
    }
}
