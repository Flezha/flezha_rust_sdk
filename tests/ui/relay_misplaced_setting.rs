//! The short form refuses a setting under a parent that does not carry it,
//! naming both.
#[flezha_runner::relay(id = "echo", sink = "out", run = flow(each = per_input))]
fn echo(input: String) -> String {
    input
}

fn main() {}
