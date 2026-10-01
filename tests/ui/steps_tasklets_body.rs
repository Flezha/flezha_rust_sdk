use flezha_runner::{ChannelBuilder, Each, OnFailure, Operator, OperatorSettings, Run};

struct Stale;

impl Operator for Stale {
    fn factory_id() -> &'static str {
        "stale"
    }

    fn tasklets(_c: &ChannelBuilder) {}

    fn settings() -> OperatorSettings {
        OperatorSettings {
            run: Run::Steps { each: Each::Once, on_failure: OnFailure::UndoAll },
            ..OperatorSettings::default()
        }
    }
}

fn main() {}
