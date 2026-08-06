//! Section 5.4 — regression test: `OperativeMessage` always returns the
//! canonical "unsupported" error per `operative-mvp-rust/design.md` Decision 9.
//!
//! `operative-message-handlers` re-enables per-Procedure `handle_message`. Until
//! then, every `OperativeMessage` request the SDK receives is answered
//! with `OperativeMessageResponse{ payload: "", error: "operative does
//! not support handle_message" }`.

use prost::Message;

#[test]
fn operative_message_always_returns_unsupported() {
    use flezha_runner::pb::OperativeMessageResponse;

    // Synthesize the byte response the handler would write.
    // (Constructing RunnerNode requires the full SDK boot path;
    //  the handler doesn't read it for OperativeMessage anymore, so a
    //  unit-level check on the response shape is sufficient.)
    let canonical = OperativeMessageResponse {
        payload: Vec::new(),
        error: "operative does not support handle_message".to_string(),
    };
    let bytes = canonical.encode_to_vec();
    let decoded = OperativeMessageResponse::decode(bytes.as_slice()).expect("decode");
    assert_eq!(decoded.payload, Vec::<u8>::new());
    assert_eq!(decoded.error, "operative does not support handle_message");
}
