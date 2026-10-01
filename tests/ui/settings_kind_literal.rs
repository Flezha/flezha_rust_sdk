//! topology.invalid_setting: a run is `flow` or `steps`; the retired kind
//! literals name no variant.
use flezha_runner::Run;

fn main() {
    let _ = Run::Bounded;
}
