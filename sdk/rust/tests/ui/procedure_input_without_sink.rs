#[flezha_runner::procedure(id = "f", input = "prices")]
fn f(a: String) -> Vec<u8> {
    a.into_bytes()
}

fn main() {}
