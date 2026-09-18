//! `sink`, `input`, `kind` and `on_full` declare an operative, so a procedure
//! refuses them, and a short form's kind is its attribute's name, never a key.
#[flezha_runner::procedure(id = "a", sink = "echo")]
fn a(input: String) -> Vec<u8> {
    input.into_bytes()
}

#[flezha_runner::procedure(id = "b", input = "prices")]
fn b(input: String) -> Vec<u8> {
    input.into_bytes()
}

#[flezha_runner::procedure(id = "c", kind = "stream")]
fn c(input: String) -> Vec<u8> {
    input.into_bytes()
}

#[flezha_runner::procedure(id = "e", on_full = drop_oldest)]
fn e(input: String) -> Vec<u8> {
    input.into_bytes()
}

#[flezha_runner::stream_operative(id = "d", kind = "stream", sink = "echo")]
fn d(input: String) -> Vec<u8> {
    input.into_bytes()
}

fn main() {}
