#[flezha_runner::procedure(init = my_init)]
fn f(a: String) -> Vec<u8> {
    a.into_bytes()
}

fn main() {}
