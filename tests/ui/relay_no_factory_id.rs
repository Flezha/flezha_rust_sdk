#[flezha_runner::relay(output = "echo")]
fn f(a: String) -> Vec<u8> {
    a.into_bytes()
}

fn main() {}
