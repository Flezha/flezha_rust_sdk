//! `sink`, `input`, `kind` and `on_full` declare an operator, so a tasklet
//! refuses them, and a short form's kind is its attribute's name, never a key.
#[flezha_runner::tasklet(id = "a", sink = "echo")]
fn a(input: String) -> Vec<u8> {
    input.into_bytes()
}

#[flezha_runner::tasklet(id = "b", input = "prices")]
fn b(input: String) -> Vec<u8> {
    input.into_bytes()
}

#[flezha_runner::tasklet(id = "c", kind = "unbounded")]
fn c(input: String) -> Vec<u8> {
    input.into_bytes()
}

#[flezha_runner::tasklet(id = "e", on_full = drop_oldest)]
fn e(input: String) -> Vec<u8> {
    input.into_bytes()
}

#[flezha_runner::unbounded_operator(id = "d", kind = "unbounded", sink = "echo")]
fn d(input: String) -> Vec<u8> {
    input.into_bytes()
}

fn main() {}
