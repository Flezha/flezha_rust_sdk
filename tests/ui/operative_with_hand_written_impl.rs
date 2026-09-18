//! ShortFormWithTopology: a short form already implements its kind's
//! authoring trait, so a hand-written topology body on the same type is a
//! second impl, which rustc refuses as conflicting.
use flezha_runner::{ChannelBuilder, StreamOperative};

#[flezha_runner::stream_operative(id = "echo", sink = "out")]
fn echo(input: String) -> Vec<u8> {
    input.into_bytes()
}

impl StreamOperative for Echo {
    fn factory_id() -> &'static str {
        "echo"
    }
    fn channels(_c: &ChannelBuilder) {}
}

fn main() {}
