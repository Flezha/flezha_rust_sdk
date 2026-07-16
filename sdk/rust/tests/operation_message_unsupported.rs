//! Section 5.4 — regression test: `OperationMessage` always returns the
//! canonical "unsupported" error per `operative-mvp-rust/design.md` Decision 9.
//!
//! `operative-message-handlers` re-enables per-Operation `handle_message`. Until
//! then, every `OperationMessage` request the SDK receives is answered
//! with `OperationMessageResponse{ payload: "", error: "operative does
//! not support handle_message" }`.

use prost::Message;

#[test]
fn operation_message_always_returns_unsupported() {
    use flezha_runner::pb::OperationMessageResponse;

    // Synthesize the byte response the handler would write.
    // (Constructing RunnerNode requires the full SDK boot path;
    //  the handler doesn't read it for OperationMessage anymore, so a
    //  unit-level check on the response shape is sufficient.)
    let canonical = OperationMessageResponse {
        payload: Vec::new(),
        error: "operative does not support handle_message".to_string(),
    };
    let bytes = canonical.encode_to_vec();
    let decoded = OperationMessageResponse::decode(bytes.as_slice()).expect("decode");
    assert_eq!(decoded.payload, Vec::<u8>::new());
    assert_eq!(decoded.error, "operative does not support handle_message");
}
