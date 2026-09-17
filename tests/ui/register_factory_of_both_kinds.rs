//! A type of both kinds has no single `Operative<K>` impl, so `register_factory`
//! cannot infer which one it was handed. That is Rust's `AmbiguousOperativeKind`.
use flezha_runner::{ChannelBuilder, Flezha, StreamOperative, WorkflowOperative};

struct BothKinds;

impl StreamOperative for BothKinds {
    fn factory_id() -> &'static str {
        "both_kinds"
    }
    fn channels(_c: &ChannelBuilder) {}
}

impl WorkflowOperative for BothKinds {
    fn factory_id() -> &'static str {
        "both_kinds"
    }
}

fn main() {
    let runner = Flezha::create_runner("ns").unwrap();
    runner.register_factory(BothKinds).unwrap();
}
