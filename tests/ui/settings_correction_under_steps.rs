//! topology.unknown_field: `correction_ms` is a setting of `effectively_once`
//! under `flow`, and a steps run has no member of that name.
use flezha_runner::{Each, OnFailure, Run};

fn main() {
    let _ = Run::Steps {
        each: Each::Once,
        on_failure: OnFailure::UndoAll,
        correction_ms: Some(5_000),
    };
}
