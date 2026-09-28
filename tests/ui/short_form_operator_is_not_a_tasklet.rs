//! A function short form declares the operator under the author's name and
//! the tasklet beside it: the operator type is not a tasklet.
use flezha_runner::{ChannelBuilder, UnboundedOperator};

#[flezha_runner::unbounded_operator(id = "do_nothing", sink = "echo")]
fn do_nothing(input: String) -> Vec<u8> {
    input.into_bytes()
}

struct Wrapper;

impl UnboundedOperator for Wrapper {
    fn factory_id() -> &'static str {
        "wrapper"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<DoNothing>().sink_named("out");
    }
}

fn main() {}
