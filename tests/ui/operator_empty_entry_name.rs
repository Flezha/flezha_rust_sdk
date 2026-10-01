flezha_runner::pipeline! {
    pub struct X;
    id: "x",
    pipeline: input::<String>("") -> A -> plugin::<Vec<u8>>("k")
}

fn main() {}
