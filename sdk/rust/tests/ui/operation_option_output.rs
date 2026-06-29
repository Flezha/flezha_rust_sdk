// Pass test: `Option<Output>` run emits on `Some`, skips on `None`.
#[flezha_executor::operation(id = "opt")]
fn opt(input: u64) -> Option<u64> {
    if input % 2 == 0 {
        Some(input)
    } else {
        None
    }
}

fn main() {}
