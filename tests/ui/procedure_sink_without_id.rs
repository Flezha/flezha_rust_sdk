#[flezha_runner::procedure(kind = "stream", sink = "echo")]
fn f(a: String) -> Vec<u8> {
    a.into_bytes()
}

fn main() {}
