//! topology.unknown_field: `ack` is a setting of `effectively_once`, and
//! `at_least_once` carries nothing, so the tree cannot be written.
use flezha_runner::{AckGranularity, Exactly};

fn main() {
    let _ = Exactly::AtLeastOnce {
        ack: AckGranularity::PerInput,
    };
}
