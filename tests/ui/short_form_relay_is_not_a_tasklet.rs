//! A function short form declares the relay under the author's name and
//! the tasklet beside it: the relay type is not a tasklet.
use flezha_runner::{ChannelBuilder, Relay};

#[flezha_runner::relay(factory_id = "do_nothing", output = "echo")]
fn do_nothing(input: String) -> Vec<u8> {
    input.into_bytes()
}

struct Wrapper;

impl Relay for Wrapper {
    fn factory_id() -> &'static str {
        "wrapper"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<DoNothing>().output_named("out");
    }
}

fn main() {}
