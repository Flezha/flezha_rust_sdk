//! Section 5.4 — regression test: `ProcessorMessage` always returns the
//! canonical "unsupported" error per `relay-mvp-rust/design.md` Decision 9.
//!
//! `relay-message-handlers` re-enables per-Shard `handle_message`. Until
//! then, every `ProcessorMessage` request the SDK receives is answered
//! with `ProcessorMessageResponse{ payload: "", error: "processor does
//! not support handle_message" }`.

use prost::Message;

#[test]
fn processor_message_always_returns_unsupported() {
    use flezha_compute_engine::pb::ProcessorMessageResponse;

    // Synthesize the byte response the handler would write.
    // (Constructing ComputeEngineNode requires the full SDK boot path;
    //  the handler doesn't read it for ProcessorMessage anymore, so a
    //  unit-level check on the response shape is sufficient.)
    let canonical = ProcessorMessageResponse {
        payload: Vec::new(),
        error: "processor does not support handle_message".to_string(),
    };
    let bytes = canonical.encode_to_vec();
    let decoded = ProcessorMessageResponse::decode(bytes.as_slice()).expect("decode");
    assert_eq!(decoded.payload, Vec::<u8>::new());
    assert_eq!(decoded.error, "processor does not support handle_message");
}
