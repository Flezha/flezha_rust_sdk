#[flezha_runner::operator(id = "chat", sink = "replies", run = steps(each = forever))]
fn chat(message: String) -> Vec<u8> {
    message.into_bytes()
}

fn main() {}
