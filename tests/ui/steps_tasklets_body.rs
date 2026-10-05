use flezha_runner::{ChannelBuilder, Each, OnFailure, Relay, RelaySettings, Run};

struct Stale;

impl Relay for Stale {
    fn factory_id() -> &'static str {
        "stale"
    }

    fn tasklets(_c: &ChannelBuilder) {}

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps { each: Each::Once, on_failure: OnFailure::UndoAll },
            ..RelaySettings::default()
        }
    }
}

fn main() {}
