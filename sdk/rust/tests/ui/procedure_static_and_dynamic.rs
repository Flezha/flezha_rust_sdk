#[flezha_runner::procedure(keys = ["a"], entities_for = pf, entity_key = sk)]
fn f(a: String) -> Vec<u8> {
    a.into_bytes()
}

fn main() {}
