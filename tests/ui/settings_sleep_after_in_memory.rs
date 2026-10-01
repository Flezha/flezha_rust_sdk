//! topology.unknown_field: `sleep_after` is a setting of `local` and `remote`,
//! and an operator kept in memory has nothing to wake from.
use flezha_runner::Durable;

fn main() {
    let _ = Durable::Memory { sleep_after: None };
}
