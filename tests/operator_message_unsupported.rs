//! `OperatorMessage` is answered with the canonical refusal. Per-Tasklet
//! `handle_message` is not implemented (`operator-message-handlers` owes it),
//! and until it is, every request must come back with an empty payload and the
//! one refusal string — never with silence, and never with a decode error.

use flezha_runner::command::CommandAction;
use flezha_runner::pb::{OperatorMessage, OperatorMessageResponse};
use prost::Message;

#[test]
fn operator_message_always_returns_unsupported() {
    let mut runner = flz_core::runner::RunnerNode::new("test-ns".to_string());
    let request = OperatorMessage {
        operator_id: "any-instance".to_string(),
        payload: b"anything".to_vec(),
    };
    let mut errors = Vec::new();

    let reply = request.handle(&mut runner, "", &mut errors);

    assert!(
        errors.is_empty(),
        "the refusal rides the reply, not `errors`"
    );
    assert_eq!(reply.payload_id, "operator_message");
    let decoded = OperatorMessageResponse::decode(reply.bytes.as_slice()).expect("decode");
    assert_eq!(decoded.payload, Vec::<u8>::new());
    assert_eq!(decoded.error, "operator does not support handle_message");
}
