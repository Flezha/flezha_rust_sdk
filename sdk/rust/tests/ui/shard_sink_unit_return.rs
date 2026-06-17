#[flezha_executor::shard(id = "x", sink = "s")]
fn f(a: String) {
    let _ = a;
}

fn main() {}
