use flezha_runner::{ChannelBuilder, Each, OnFailure, Relay, RelaySettings, Run};

struct ClassLadder;

impl Relay for ClassLadder {
    fn factory_id() -> &'static str {
        "class_ladder"
    }

    fn channels(_c: &ChannelBuilder) {}

    fn retry_attempts() -> u32 {
        5
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps { each: Each::Once, on_failure: OnFailure::UndoAll },
            ..RelaySettings::default()
        }
    }
}

fn main() {}
