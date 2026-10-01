//! topology.unknown_field: `exactly` is a setting of `flow`, and a steps run
//! has no member of that name.
use flezha_runner::{Each, Exactly, OnFailure, Run};

fn main() {
    let _ = Run::Steps {
        each: Each::Once,
        on_failure: OnFailure::UndoAll,
        exactly: Exactly::effectively_once(),
    };
}
