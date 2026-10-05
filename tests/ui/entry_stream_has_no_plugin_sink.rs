//! An entry stream carries the shared surface, not the producer-side one:
//! `sink` / `named` / `codec` / `keyed` stay on a tasklet's
//! output stream.
struct Op;
impl flezha_runner::Relay for Op {
    fn factory_id() -> &'static str {
        "entry_no_sink"
    }
    fn channels(c: &flezha_runner::ChannelBuilder) {
        c.input::<String>("in").sink();
    }
}

fn main() {}
