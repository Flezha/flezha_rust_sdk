//! topology.unknown_field: `each` is a setting of `steps`, and a flow run has
//! no member of that name, so the tree cannot be written.
use flezha_runner::{Each, Exactly, Run};

fn main() {
    let _ = Run::Flow {
        exactly: Exactly::AtLeastOnce,
        each: Each::PerInput,
    };
}
