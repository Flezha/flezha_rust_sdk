#[flezha_runner::stream_operative(sink = "echo")]
fn f(a: String) -> Vec<u8> {
    a.into_bytes()
}

fn main() {}
