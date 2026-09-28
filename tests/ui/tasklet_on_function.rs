//! `#[flezha::tasklet]` sits on an `impl Tasklet` block; on a function it
//! directs the author to the block or to the short-form attributes.
#[flezha_runner::tasklet(name = "echo")]
fn echo(input: String) -> String {
    input
}

fn main() {}
