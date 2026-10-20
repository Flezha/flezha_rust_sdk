#[flezha_runner::relay(factory_id = "f", output = "s", input = "")]
fn f(a: String) -> Vec<u8> {
    a.into_bytes()
}

fn main() {}
