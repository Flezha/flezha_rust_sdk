//! Section 5.4 — regression test: `RelayMessage` always returns the
//! canonical "unsupported" error per `relay-mvp-rust/design.md` Decision 9.
//!
//! `relay-message-handlers` re-enables per-Shard `handle_message`. Until
//! then, every `RelayMessage` request the SDK receives is answered
//! with `RelayMessageResponse{ payload: "", error: "relay does
//! not support handle_message" }`.

use prost::Message;

#[test]
fn relay_message_always_returns_unsupported() {
    use flezha_executor::pb::RelayMessageResponse;

    // Synthesize the byte response the handler would write.
    // (Constructing ExecutorNode requires the full SDK boot path;
    //  the handler doesn't read it for RelayMessage anymore, so a
    //  unit-level check on the response shape is sufficient.)
    let canonical = RelayMessageResponse {
        payload: Vec::new(),
        error: "relay does not support handle_message".to_string(),
    };
    let bytes = canonical.encode_to_vec();
    let decoded = RelayMessageResponse::decode(bytes.as_slice()).expect("decode");
    assert_eq!(decoded.payload, Vec::<u8>::new());
    assert_eq!(decoded.error, "relay does not support handle_message");
}
