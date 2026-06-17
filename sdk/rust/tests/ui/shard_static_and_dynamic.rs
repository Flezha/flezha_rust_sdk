#[flezha_executor::shard(keys = ["a"], partition_for = pf, shard_key = sk)]
fn f(a: String) -> Vec<u8> {
    a.into_bytes()
}

fn main() {}
