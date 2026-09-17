flezha_runner::operative! {
    pub struct X;
    id: "x",
    pipeline: input::<String>("") -> A -> plugin::<Vec<u8>>("k")
}

fn main() {}
