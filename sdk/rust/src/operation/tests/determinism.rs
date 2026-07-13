//! Ctx-level determinism surface: `now()` serves one per-invocation value,
//! `stable_id()` derivation, keyed-emit enforcement, and the reserved
//! `flz.` metadata namespace.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::operation::ctx::{EmitSink, OperationCtx, StableId};
use crate::operation::error::OperationError;

/// Minimal sink: records what reached it, configurable channel shape.
#[derive(Default)]
struct StubSink {
    keyed: bool,
    plugin_bound: bool,
    keys_seen: Vec<Option<String>>,
    metas_seen: Vec<Option<HashMap<String, String>>>,
}

impl EmitSink for StubSink {
    fn emit_inter_operation(
        &mut self,
        _value: Arc<dyn Any + Send + Sync>,
    ) -> Result<(), OperationError> {
        self.keys_seen.push(None);
        self.metas_seen.push(None);
        Ok(())
    }

    fn emit_plugin(
        &mut self,
        _value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), OperationError> {
        self.keys_seen.push(key);
        self.metas_seen.push(meta);
        Ok(())
    }

    fn channel_name(&self) -> &str {
        "stub"
    }

    fn is_plugin_bound(&self) -> bool {
        self.plugin_bound
    }

    fn is_keyed(&self) -> bool {
        self.keyed
    }
}

const IDENTITY: &[u8] = b"instance/op/key";

#[test]
fn now_returns_the_injected_per_invocation_value_on_every_call() {
    let mut sink = StubSink::default();
    let ctx = OperationCtx::new(&mut sink, 1720000000000, 7, IDENTITY);
    assert_eq!(ctx.now(), 1720000000000);
    assert_eq!(ctx.now(), 1720000000000);
}

#[test]
fn stable_id_is_distinct_within_an_invocation() {
    let mut sink = StubSink::default();
    let ctx = OperationCtx::new(&mut sink, 0, 7, IDENTITY);
    let a = ctx.stable_id();
    let b = ctx.stable_id();
    assert_ne!(a, b, "two calls in one invocation must differ");
}

#[test]
fn stable_id_is_identical_for_identical_replay_inputs() {
    let mut sink_a = StubSink::default();
    let mut sink_b = StubSink::default();
    let ctx_a = OperationCtx::new(&mut sink_a, 0, 42, IDENTITY);
    let ctx_b = OperationCtx::new(&mut sink_b, 0, 42, IDENTITY);
    // The n-th call in a replayed invocation equals the n-th original call.
    assert_eq!(ctx_a.stable_id(), ctx_b.stable_id());
    assert_eq!(ctx_a.stable_id(), ctx_b.stable_id());
}

#[test]
fn stable_id_depends_on_segment_identity_and_seq() {
    let base = StableId::derive(IDENTITY, 42, 0);
    assert_ne!(base, StableId::derive(b"other/op/key", 42, 0));
    assert_ne!(base, StableId::derive(IDENTITY, 43, 0));
}

#[test]
fn stable_id_displays_as_32_hex_chars() {
    let id = StableId::derive(IDENTITY, 1, 0);
    let rendered = id.to_string();
    assert_eq!(rendered.len(), 32);
    assert!(rendered.chars().all(|c| c.is_ascii_hexdigit()));
}

/// Cross-language fixture: the Java SDK derives `StableId` with a hand-rolled
/// 128-bit FNV-1a (no `u128` in Java). A user may embed the rendered id in an
/// emitted key, so the two SDKs must agree bit for bit. The twin assertion
/// lives in Java's `StableIdTest`.
#[test]
fn stable_id_matches_the_cross_language_fixture() {
    assert_eq!(
        StableId::derive(b"instance/op/key", 42, 0).to_string(),
        "bbf81fd4955afd1724203700a240e822"
    );
    assert_eq!(
        StableId::derive(b"instance/op/key", 42, 1).to_string(),
        "bbf81fd4965afd1724203700a240e95d"
    );
    assert_eq!(
        StableId::derive(b"", 0, 0).to_string(),
        "15915375576d2f389a938dc3a9940c9d"
    );
}

#[test]
fn plain_emit_on_keyed_channel_is_rejected() {
    let mut sink = StubSink {
        keyed: true,
        plugin_bound: true,
        ..Default::default()
    };
    let mut ctx = OperationCtx::new(&mut sink, 0, 0, IDENTITY);
    let err = ctx.emit::<String>("v".into()).unwrap_err();
    assert!(matches!(
        err,
        OperationError::KeyRequiredOnKeyedChannel { .. }
    ));
    let err = ctx
        .emit_with_meta::<String>("v".into(), HashMap::new())
        .unwrap_err();
    assert!(matches!(
        err,
        OperationError::KeyRequiredOnKeyedChannel { .. }
    ));
    assert!(sink.keys_seen.is_empty(), "nothing may be emitted");
}

#[test]
fn emit_keyed_on_non_keyed_channel_is_rejected() {
    let mut sink = StubSink {
        plugin_bound: true,
        ..Default::default()
    };
    let mut ctx = OperationCtx::new(&mut sink, 0, 0, IDENTITY);
    let err = ctx.emit_keyed::<String>("k", "v".into()).unwrap_err();
    assert!(matches!(
        err,
        OperationError::KeyNotAllowedOnNonKeyedChannel { .. }
    ));
    assert!(sink.keys_seen.is_empty(), "nothing may be emitted");
}

#[test]
fn emit_keyed_passes_the_natural_key_to_the_sink() {
    let mut sink = StubSink {
        keyed: true,
        plugin_bound: true,
        ..Default::default()
    };
    let mut ctx = OperationCtx::new(&mut sink, 0, 0, IDENTITY);
    ctx.emit_keyed::<String>("BTC/1m/1720000000", "candle".into())
        .expect("keyed emit on keyed channel");
    assert_eq!(sink.keys_seen, vec![Some("BTC/1m/1720000000".to_string())]);
}

#[test]
fn reserved_flz_metadata_keys_are_rejected_on_all_meta_variants() {
    let mut sink = StubSink {
        plugin_bound: true,
        ..Default::default()
    };
    let mut meta = HashMap::new();
    meta.insert("flz.seq".to_string(), "999.0".to_string());
    {
        let mut ctx = OperationCtx::new(&mut sink, 0, 0, IDENTITY);
        let err = ctx
            .emit_with_meta::<String>("v".into(), meta.clone())
            .unwrap_err();
        assert!(matches!(err, OperationError::ReservedMetadataKey { key } if key == "flz.seq"));
    }
    let mut keyed_sink = StubSink {
        keyed: true,
        plugin_bound: true,
        ..Default::default()
    };
    {
        let mut ctx = OperationCtx::new(&mut keyed_sink, 0, 0, IDENTITY);
        let err = ctx
            .emit_keyed_with_meta::<String>("k", "v".into(), meta)
            .unwrap_err();
        assert!(matches!(err, OperationError::ReservedMetadataKey { key } if key == "flz.seq"));
    }
    assert!(sink.keys_seen.is_empty());
    assert!(keyed_sink.keys_seen.is_empty());
}

#[test]
fn non_reserved_metadata_still_flows() {
    let mut sink = StubSink {
        keyed: true,
        plugin_bound: true,
        ..Default::default()
    };
    let mut meta = HashMap::new();
    meta.insert("user".to_string(), "v".to_string());
    let mut ctx = OperationCtx::new(&mut sink, 0, 0, IDENTITY);
    ctx.emit_keyed_with_meta::<String>("k", "v".into(), meta)
        .expect("user metadata without flz. prefix is fine");
    assert_eq!(sink.metas_seen.len(), 1);
    assert_eq!(
        sink.metas_seen[0].as_ref().unwrap().get("user"),
        Some(&"v".to_string())
    );
}
