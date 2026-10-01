use flezha_runner::{ChannelBuilder, Each, OnFailure, Operator, OperatorSettings, Run};

struct ClassLadder;

impl Operator for ClassLadder {
    fn factory_id() -> &'static str {
        "class_ladder"
    }

    fn channels(_c: &ChannelBuilder) {}

    fn retry_attempts() -> u32 {
        5
    }

    fn settings() -> OperatorSettings {
        OperatorSettings {
            run: Run::Steps { each: Each::Once, on_failure: OnFailure::UndoAll },
            ..OperatorSettings::default()
        }
    }
}

fn main() {}
