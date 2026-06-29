#[flezha_executor::operation(keys = ["a"], segment_for = pf, operation_key = sk)]
fn f(a: String) -> Vec<u8> {
    a.into_bytes()
}

fn main() {}
