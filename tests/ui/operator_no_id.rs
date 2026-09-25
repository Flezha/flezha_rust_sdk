#[flezha_runner::unbounded_operator(sink = "echo")]
fn f(a: String) -> Vec<u8> {
    a.into_bytes()
}

fn main() {}
