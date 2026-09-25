flezha_runner::operator! {
    pub struct X;
    id: "x",
    pipeline: input::<String>("") -> A -> plugin::<Vec<u8>>("k")
}

fn main() {}
