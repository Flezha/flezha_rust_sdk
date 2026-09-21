#[flezha_runner::tasklet]
fn f(a: String, s: &String) -> Vec<u8> {
    let _ = s;
    a.into_bytes()
}

fn main() {}
