//! An entry stream carries the shared surface, not the producer-side one:
//! `output` / `also_output` / `named` / `codec` / `keyed` stay on a
//! procedure's output stream.
struct Op;
impl flezha_runner::UnboundedOperative for Op {
    fn factory_id() -> &'static str {
        "entry_no_sink"
    }
    fn channels(c: &flezha_runner::ChannelBuilder) {
        c.input::<String>("in").output("plugin");
    }
}

fn main() {}
