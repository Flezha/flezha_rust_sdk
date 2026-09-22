#[flezha_runner::workflow_operative(id = "chat", sink = "replies", loop_mode = "forever")]
fn chat(message: String) -> Vec<u8> {
    message.into_bytes()
}

fn main() {}
