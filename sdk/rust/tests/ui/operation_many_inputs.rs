#[flezha_runner::operation]
fn f(a: String, b: String) -> Vec<u8> {
    let _ = b;
    a.into_bytes()
}

fn main() {}
