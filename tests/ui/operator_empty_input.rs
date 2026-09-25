#[flezha_runner::unbounded_operator(id = "f", sink = "s", input = "")]
fn f(a: String) -> Vec<u8> {
    a.into_bytes()
}

fn main() {}
