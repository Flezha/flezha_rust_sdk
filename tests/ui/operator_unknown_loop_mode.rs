#[flezha_runner::bounded_operator(id = "chat", sink = "replies", loop_mode = "forever")]
fn chat(message: String) -> Vec<u8> {
    message.into_bytes()
}

fn main() {}
