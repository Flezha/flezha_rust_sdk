#[flezha_executor::shard(keys = ["a", "b"])]
fn f(a: String) -> Vec<u8> {
    a.into_bytes()
}

fn main() {}
