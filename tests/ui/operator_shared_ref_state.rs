//! A short form's shape refusals name the attribute the author wrote.
#[derive(Default)]
struct S;

#[flezha_runner::operator(id = "f", sink = "out", run = steps)]
fn f(a: String, s: &S) -> Vec<u8> {
    let _ = s;
    a.into_bytes()
}

fn main() {}
