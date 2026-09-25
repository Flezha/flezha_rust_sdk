use flezha_runner::{ChannelBuilder, BoundedOperator};

struct Stale;

impl BoundedOperator for Stale {
    fn factory_id() -> &'static str {
        "stale"
    }

    fn tasklets(_c: &ChannelBuilder) {}
}

fn main() {}
