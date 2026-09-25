//! A type of both kinds has no single `Operator<K>` impl, so `register_factory`
//! cannot infer which one it was handed. That is Rust's `AmbiguousOperatorKind`.
use flezha_runner::{ChannelBuilder, Flezha, UnboundedOperator, BoundedOperator};

struct BothKinds;

impl UnboundedOperator for BothKinds {
    fn factory_id() -> &'static str {
        "both_kinds"
    }
    fn channels(_c: &ChannelBuilder) {}
}

impl BoundedOperator for BothKinds {
    fn factory_id() -> &'static str {
        "both_kinds"
    }
    fn channels(_c: &ChannelBuilder) {}
}

fn main() {
    let runner = Flezha::create_runner("ns").unwrap();
    runner.register_factory(BothKinds).unwrap();
}
