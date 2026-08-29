//! `OperativeMessage` is answered with the canonical refusal. Per-Procedure
//! `handle_message` is not implemented (`operative-message-handlers` owes it),
//! and until it is, every request must come back with an empty payload and the
//! one refusal string — never with silence, and never with a decode error.

use flezha_runner::command::CommandAction;
use flezha_runner::pb::{OperativeMessage, OperativeMessageResponse};
use prost::Message;

#[test]
fn operative_message_always_returns_unsupported() {
    let mut runner = flz_core::runner::RunnerNode::new("test-ns".to_string());
    let request = OperativeMessage {
        operative_id: "any-instance".to_string(),
        payload: b"anything".to_vec(),
    };
    let mut errors = Vec::new();

    let reply = request.handle(&mut runner, "", &mut errors);

    assert!(
        errors.is_empty(),
        "the refusal rides the reply, not `errors`"
    );
    assert_eq!(reply.payload_id, "operative_message");
    let decoded = OperativeMessageResponse::decode(reply.bytes.as_slice()).expect("decode");
    assert_eq!(decoded.payload, Vec::<u8>::new());
    assert_eq!(decoded.error, "operative does not support handle_message");
}
