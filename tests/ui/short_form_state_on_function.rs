//! A function short form takes no state: a stateful one-tasklet operator is
//! written on an `impl Tasklet` block under the same attribute.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Seen {
    count: u64,
}

#[flezha_runner::operator(id = "store_input")]
fn store_input(input: String, state: &mut Seen) -> String {
    state.count += 1;
    input
}

fn main() {}
