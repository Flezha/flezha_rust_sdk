use flezha_runner::{ChannelBuilder, WorkflowOperative};

struct ClassLadder;

impl WorkflowOperative for ClassLadder {
    fn factory_id() -> &'static str {
        "class_ladder"
    }

    fn channels(_c: &ChannelBuilder) {}

    fn retry_attempts() -> u32 {
        5
    }
}

fn main() {}
