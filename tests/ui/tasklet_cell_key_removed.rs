//! Dynamic keying moved to the wiring, so the authoring sugar offers no
//! `cell_key` hook and declaring one names its replacement.
#[flezha_runner::tasklet(id = "p", cell_key = sk)]
fn f(a: String) -> Vec<u8> {
    a.into_bytes()
}

fn sk(a: &String) -> Option<String> {
    Some(a.clone())
}

fn main() {}
