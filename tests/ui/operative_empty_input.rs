#[flezha_runner::stream_operative(id = "f", sink = "s", input = "")]
fn f(a: String) -> Vec<u8> {
    a.into_bytes()
}

fn main() {}
