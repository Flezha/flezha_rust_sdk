#[flezha_runner::procedure(id = "f", kind = "saga", sink = "echo")]
fn f(a: String) -> Vec<u8> {
    a.into_bytes()
}

fn main() {}
