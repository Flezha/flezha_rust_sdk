flezha_runner::pipeline! {
    pub struct X;
    id: "x",
    pipeline: input::<String> -> A -> plugin::<Vec<u8>>("k"), input::<String> -> B -> plugin::<Vec<u8>>("k2")
}

fn main() {}
