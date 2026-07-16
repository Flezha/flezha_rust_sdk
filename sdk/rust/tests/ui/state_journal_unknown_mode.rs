use std::collections::HashMap;

#[derive(flezha_runner::State)]
struct S {
    #[journal(bogus)]
    m: HashMap<String, f64>,
}

fn main() {}
