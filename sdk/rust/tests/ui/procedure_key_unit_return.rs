// `key` needs an output to key; declaring it on a unit-return procedure is an
// authoring error, not a silently ignored attribute.
fn sk(_out: &u64) -> String {
    String::new()
}

#[flezha_runner::procedure(id = "p", key = sk)]
fn p(_input: u64) {}

fn main() {}
