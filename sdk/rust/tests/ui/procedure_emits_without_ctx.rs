// Without a context parameter the return type declares the output; `emits`
// would be a second answer to the same question.
#[flezha_runner::procedure(id = "p", emits = u64)]
fn p(input: u64) -> u64 {
    input
}

fn main() {}
