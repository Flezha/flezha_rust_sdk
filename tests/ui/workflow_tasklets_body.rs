use flezha_runner::{ChannelBuilder, WorkflowOperative};

struct Stale;

impl WorkflowOperative for Stale {
    fn factory_id() -> &'static str {
        "stale"
    }

    fn tasklets(_c: &ChannelBuilder) {}
}

fn main() {}
