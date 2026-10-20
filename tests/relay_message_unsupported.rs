//! `RelayMessage` is answered with the canonical refusal. Per-Tasklet
//! `handle_message` is not implemented (`relay-message-handlers` owes it),
//! and until it is, every request must come back with an empty payload and the
//! one refusal code — never with silence, and never with a decode error.

use flz_core::command::CommandAction;
use flz_core::pb::{RelayMessage, RelayMessageResponse};
use prost::Message;

#[test]
fn relay_message_always_returns_unsupported() {
    let mut runner = flz_core::runner::RunnerNode::new("test-ns".to_string());
    let request = RelayMessage {
        relay_id: "any-instance".to_string(),
        payload: b"anything".to_vec(),
    };
    let mut errors = Vec::new();

    let reply = request.handle(&mut runner, "", &mut errors);

    assert!(
        errors.is_empty(),
        "the refusal rides the reply, not `errors`"
    );
    assert_eq!(reply.payload_id, "relay_message");
    let decoded = RelayMessageResponse::decode(reply.bytes.as_slice()).expect("decode");
    assert_eq!(decoded.payload, Vec::<u8>::new());
    assert_eq!(
        decoded.error.map(|e| e.code).as_deref(),
        Some("runtime.relay_message_unsupported")
    );
}
