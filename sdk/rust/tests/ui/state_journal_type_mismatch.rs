use std::collections::HashMap;

#[derive(flezha_runner::State)]
struct S {
    #[journal(map_vec)]
    m: HashMap<String, f64>,
}

fn main() {}
