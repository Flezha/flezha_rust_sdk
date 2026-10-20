#[flezha_runner::relay(factory_id = "chat", output = "replies", run = steps(each = forever))]
fn chat(message: String) -> Vec<u8> {
    message.into_bytes()
}

fn main() {}
