//! topology.unknown_field: `ack` is a setting of `effectively_once` under
//! `flow`, and a steps run has no member of that name.
use flezha_runner::{AckGranularity, Each, OnFailure, Run};

fn main() {
    let _ = Run::Steps {
        each: Each::Once,
        on_failure: OnFailure::UndoAll,
        ack: AckGranularity::PerInput,
    };
}
