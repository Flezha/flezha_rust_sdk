//! An entry stream carries the shared surface, not the producer-side one:
//! `sink` / `sealed` / `named` / `codec` / `keyed` stay on a procedure's
//! output stream.
struct Op;
impl flezha_runner::StreamOperative for Op {
    fn factory_id() -> &'static str {
        "entry_no_sink"
    }
    fn channels(c: &flezha_runner::ChannelBuilder) {
        c.input::<String>("in").sink("plugin");
    }
}

fn main() {}
