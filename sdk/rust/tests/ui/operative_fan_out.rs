flezha_executor::operative! {
    id: "x",
    pipeline: input::<String> -> A -> plugin::<Vec<u8>>("k"), input::<String> -> B -> plugin::<Vec<u8>>("k2")
}

fn main() {}
