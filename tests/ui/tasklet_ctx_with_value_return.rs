// A context parameter makes the body responsible for every emit; a value
// return would add a second, invisible emit path to the same tasklet.
#[flezha_runner::tasklet(id = "p")]
fn p(input: u64, _ctx: &mut flezha_runner::TaskletCtx) -> u64 {
    input
}

fn main() {}
