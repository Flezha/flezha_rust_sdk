//! Unit tests for plugin egress (`sink` / `sink_with`): the hybrid channel
//! (a sink with consumers wired after it), the plugin-bound terminal (a sink
//! with none), and the equivalence of the two declarations on the wire.
//!
//! Each test uses the live dispatcher: an entry channel feeds bytes
//! into a producer Procedure, which calls `ctx.emit(typed_value)` on a
//! channel wired to one or more inter-procedure consumers and a plugin
//! sink. The consumer-side delivery is observed via static state in
//! the consumer Procedure; the plugin-side delivery is observed by reading
//! the receiving end of the unix socket pair and parsing wire frames.

use std::io::Read;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex, OnceLock};

use flezha_runner::channel::{ChannelBuilder, ChannelKind};
use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;

// ---------- shared scaffolding ----------

mod common;
use common::prelude::*;
use common::{setup, stays_quiet, wait_until};

type ReceivedFrame = (String, Vec<u8>, HashMap<String, String>);

/// Build a unix socket pair, wrap the producer side as a `FlezhaStream`,
/// and spawn a reader thread on the receiver side that parses incoming
/// payload frames and pushes them onto the returned `Mutex<Vec<...>>`.
fn duplex_stream_with_reader() -> (Arc<FlezhaStream>, Arc<Mutex<Vec<ReceivedFrame>>>) {
    let (a, b) = UnixStream::pair().expect("UnixStream::pair");
    let received = Arc::new(Mutex::new(Vec::new()));
    let received_clone = Arc::clone(&received);
    std::thread::spawn(move || {
        let mut reader = b;
        loop {
            match payloader::extract_payload(&mut reader) {
                Ok(frame) => received_clone.lock().unwrap().push(frame),
                Err(_) => return,
            }
        }
    });
    (Arc::new(FlezhaStream::Unix(a)), received)
}

fn dispatch_input(
    handle: &OperativeDispatchHandle,
    instance_id: &str,
    stream: &Arc<FlezhaStream>,
    input: &str,
) {
    let mut md = HashMap::new();
    md.insert("channel".to_string(), "input".to_string());
    handle.dispatch(
        instance_id,
        input.as_bytes().to_vec(),
        &md,
        Arc::clone(stream),
    );
}

// ---------- 2.5: 1 consumer + plugin sink ----------

fn single_consumer_observed() -> &'static Mutex<Vec<u32>> {
    static CELL: OnceLock<Mutex<Vec<u32>>> = OnceLock::new();
    CELL.get_or_init(|| Mutex::new(Vec::new()))
}

test_procedure!(SingleProducer, "hybrid_single_producer", String => u32, (), |input, _state, ctx| {
    let n: u32 = input.parse().unwrap_or(0);
    ctx.emit::<u32>(n)
});

test_procedure!(SingleConsumer, "hybrid_single_consumer", u32 => (), (), |input, _state, _ctx| {
    single_consumer_observed().lock().unwrap().push(input);
    Ok(())
});

struct SingleHybridOperative;
impl StreamOperative for SingleHybridOperative {
    fn factory_id() -> &'static str {
        "hybrid_single"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<SingleProducer>()
            .named("hybrid_out")
            .sink_with("plugin_a", |v: &u32| v.to_be_bytes().to_vec())
            .into::<SingleConsumer>();
    }
}

#[test]
fn hybrid_single_consumer_receives_typed_and_plugin_bytes() {
    single_consumer_observed().lock().unwrap().clear();
    let (handle, _instance) = setup::<SingleHybridOperative>("hyb-single");
    let (stream, received) = duplex_stream_with_reader();

    dispatch_input(&handle, "hyb-single", &stream, "42");

    assert!(
        wait_until(
            || single_consumer_observed().lock().unwrap().len() == 1
                && received.lock().unwrap().len() == 1,
            Duration::from_secs(5)
        ),
        "consumer or plugin frame did not arrive"
    );

    assert_eq!(*single_consumer_observed().lock().unwrap(), vec![42u32]);
    let frames = received.lock().unwrap();
    let (id, body, meta) = frames.first().unwrap();
    assert_eq!(id, "hyb-single");
    assert_eq!(body, &42u32.to_be_bytes().to_vec());
    assert_eq!(meta.get("channel").map(|s| s.as_str()), Some("hybrid_out"));
}

// ---------- 2.6: 3 consumers, single serializer call ----------

static SERIALIZER_CALLS: AtomicUsize = AtomicUsize::new(0);

fn three_consumer_observed_a() -> &'static Mutex<Vec<u32>> {
    static C: OnceLock<Mutex<Vec<u32>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Vec::new()))
}
fn three_consumer_observed_b() -> &'static Mutex<Vec<u32>> {
    static C: OnceLock<Mutex<Vec<u32>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Vec::new()))
}
fn three_consumer_observed_c() -> &'static Mutex<Vec<u32>> {
    static C: OnceLock<Mutex<Vec<u32>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Vec::new()))
}

test_procedure!(ThreeProducer, "hybrid_three_producer", String => u32, (), |input, _state, ctx| {
    let n: u32 = input.parse().unwrap_or(0);
    ctx.emit::<u32>(n)
});

test_procedure!(ThreeConsumerA, "hybrid_three_a", u32 => (), (), |input, _state, _ctx| {
    three_consumer_observed_a().lock().unwrap().push(input);
    Ok(())
});
test_procedure!(ThreeConsumerB, "hybrid_three_b", u32 => (), (), |input, _state, _ctx| {
    three_consumer_observed_b().lock().unwrap().push(input);
    Ok(())
});
test_procedure!(ThreeConsumerC, "hybrid_three_c", u32 => (), (), |input, _state, _ctx| {
    three_consumer_observed_c().lock().unwrap().push(input);
    Ok(())
});

struct ThreeHybridOperative;
impl StreamOperative for ThreeHybridOperative {
    fn factory_id() -> &'static str {
        "hybrid_three"
    }
    fn channels(c: &ChannelBuilder) {
        let out = c
            .input::<String>("input")
            .into::<ThreeProducer>()
            .named("three_out")
            .sink_with("plugin_three", |v: &u32| {
                SERIALIZER_CALLS.fetch_add(1, Ordering::SeqCst);
                v.to_be_bytes().to_vec()
            });
        out.into::<ThreeConsumerA>();
        out.into::<ThreeConsumerB>();
        out.into::<ThreeConsumerC>();
    }
}

#[test]
fn hybrid_three_consumers_single_serializer_call() {
    three_consumer_observed_a().lock().unwrap().clear();
    three_consumer_observed_b().lock().unwrap().clear();
    three_consumer_observed_c().lock().unwrap().clear();
    SERIALIZER_CALLS.store(0, Ordering::SeqCst);

    let (handle, _instance) = setup::<ThreeHybridOperative>("hyb-three");
    let (stream, received) = duplex_stream_with_reader();

    dispatch_input(&handle, "hyb-three", &stream, "7");

    assert!(
        wait_until(
            || three_consumer_observed_a().lock().unwrap().len() == 1
                && three_consumer_observed_b().lock().unwrap().len() == 1
                && three_consumer_observed_c().lock().unwrap().len() == 1
                && received.lock().unwrap().len() == 1,
            Duration::from_secs(5)
        ),
        "delivery to all 3 consumers + plugin did not complete"
    );

    assert_eq!(*three_consumer_observed_a().lock().unwrap(), vec![7u32]);
    assert_eq!(*three_consumer_observed_b().lock().unwrap(), vec![7u32]);
    assert_eq!(*three_consumer_observed_c().lock().unwrap(), vec![7u32]);
    assert_eq!(
        SERIALIZER_CALLS.load(Ordering::SeqCst),
        1,
        "serializer must run exactly once regardless of consumer count"
    );
}

// ---------- 2.7: panicking serializer is caught, dispatcher survives ----------

fn panic_consumer_observed() -> &'static Mutex<Vec<u32>> {
    static C: OnceLock<Mutex<Vec<u32>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Vec::new()))
}

fn panic_emit_results() -> &'static Mutex<Vec<Result<(), String>>> {
    static C: OnceLock<Mutex<Vec<Result<(), String>>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Vec::new()))
}

test_procedure!(PanicProducer, "hybrid_panic_producer", String => u32, (), |input, _state, ctx| {
    let n: u32 = input.parse().unwrap_or(0);
    let outcome = ctx.emit::<u32>(n).map_err(|e| format!("{e}"));
    panic_emit_results().lock().unwrap().push(outcome);
    Ok(())
});

test_procedure!(PanicConsumer, "hybrid_panic_consumer", u32 => (), (), |input, _state, _ctx| {
    panic_consumer_observed().lock().unwrap().push(input);
    Ok(())
});

struct PanicHybridOperative;
impl StreamOperative for PanicHybridOperative {
    fn factory_id() -> &'static str {
        "hybrid_panic"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<PanicProducer>()
            .named("panic_out")
            .sink_with("plugin_panic", |v: &u32| {
                if *v == 13 {
                    panic!("test serializer rejected 13");
                }
                v.to_be_bytes().to_vec()
            })
            .into::<PanicConsumer>();
    }
}

#[test]
fn hybrid_serializer_panic_is_caught() {
    panic_consumer_observed().lock().unwrap().clear();
    panic_emit_results().lock().unwrap().clear();

    let (handle, _instance) = setup::<PanicHybridOperative>("hyb-panic");
    let (stream, received) = duplex_stream_with_reader();

    // First emit panics in the serializer.
    dispatch_input(&handle, "hyb-panic", &stream, "13");
    // Second emit succeeds — proves the dispatcher thread is still alive.
    dispatch_input(&handle, "hyb-panic", &stream, "5");

    assert!(
        wait_until(
            || panic_consumer_observed().lock().unwrap().len() == 2
                && panic_emit_results().lock().unwrap().len() == 2,
            Duration::from_secs(5)
        ),
        "consumer did not see both inputs"
    );

    // Both consumer fan-outs completed (panic happens after).
    assert_eq!(
        *panic_consumer_observed().lock().unwrap(),
        vec![13u32, 5u32]
    );

    let outcomes = panic_emit_results().lock().unwrap().clone();
    let first_msg = outcomes[0].as_ref().expect_err("first emit must error");
    assert!(
        first_msg.contains("panic_out") && first_msg.contains("test serializer rejected 13"),
        "expected PluginSerializerPanic message; got: {first_msg}"
    );
    assert!(outcomes[1].is_ok(), "second emit should succeed");

    // Only the second emit produced a wire frame.
    assert!(
        wait_until(
            || received.lock().unwrap().len() == 1,
            Duration::from_secs(2)
        ),
        "expected exactly one wire frame from the surviving emit"
    );
    let frames = received.lock().unwrap();
    let (_, body, _) = frames.first().unwrap();
    assert_eq!(body, &5u32.to_be_bytes().to_vec());
}

// ---------- 2.8: duplicate sink_with rejected at registration ----------

test_procedure!(DupProducer, "hybrid_dup_producer", String => u32, (), |_input, _state, _ctx| {
    Ok(())
});

struct DupHybridOperative;
impl StreamOperative for DupHybridOperative {
    fn factory_id() -> &'static str {
        "hybrid_dup"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<DupProducer>()
            .named("dup_out")
            .sink_with("plugin_a", |v: &u32| v.to_be_bytes().to_vec())
            .sink_with("plugin_b", |v: &u32| v.to_be_bytes().to_vec());
    }
}

#[test]
fn hybrid_duplicate_plugin_sink_is_rejected() {
    let result = build_topology_for::<DupHybridOperative>();
    let err = match result {
        Ok(_) => panic!("expected topology error"),
        Err(e) => e,
    };
    let msg = format!("{err:?}");
    assert!(
        msg.contains("DuplicatePluginSink") && msg.contains("dup_out"),
        "expected DuplicatePluginSink for dup_out, got: {msg}"
    );
}

// ---------- 3.3: emit_with_meta on a hybrid channel ----------

fn meta_consumer_observed() -> &'static Mutex<Vec<u32>> {
    static C: OnceLock<Mutex<Vec<u32>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Vec::new()))
}

test_procedure!(MetaProducer, "hybrid_meta_producer", String => u32, (), |input, _state, ctx| {
    let n: u32 = input.parse().unwrap_or(0);
    let mut meta = HashMap::new();
    meta.insert("trace_id".to_string(), "abc123".to_string());
    // Reserved key: must be silently dropped from user-supplied
    // metadata and replaced by the real channel name.
    meta.insert("channel".to_string(), "spoof".to_string());
    ctx.emit_with_meta::<u32>(n, meta)
});

test_procedure!(MetaConsumer, "hybrid_meta_consumer", u32 => (), (), |input, _state, _ctx| {
    meta_consumer_observed().lock().unwrap().push(input);
    Ok(())
});

struct MetaHybridOperative;
impl StreamOperative for MetaHybridOperative {
    fn factory_id() -> &'static str {
        "hybrid_meta"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<MetaProducer>()
            .named("meta_out")
            .sink_with("plugin_meta", |v: &u32| v.to_be_bytes().to_vec())
            .into::<MetaConsumer>();
    }
}

#[test]
fn emit_with_meta_on_hybrid_channel_forwards_meta_to_plugin_only() {
    meta_consumer_observed().lock().unwrap().clear();
    let (handle, _instance) = setup::<MetaHybridOperative>("hyb-meta");
    let (stream, received) = duplex_stream_with_reader();

    dispatch_input(&handle, "hyb-meta", &stream, "9");

    assert!(
        wait_until(
            || meta_consumer_observed().lock().unwrap().len() == 1
                && received.lock().unwrap().len() == 1,
            Duration::from_secs(5)
        ),
        "consumer or plugin frame did not arrive"
    );

    assert_eq!(*meta_consumer_observed().lock().unwrap(), vec![9u32]);
    let frames = received.lock().unwrap();
    let (_, body, meta) = frames.first().unwrap();
    assert_eq!(body, &9u32.to_be_bytes().to_vec());
    assert_eq!(
        meta.get("trace_id").map(|s| s.as_str()),
        Some("abc123"),
        "trace_id metadata must reach the plugin"
    );
    assert_eq!(
        meta.get("channel").map(|s| s.as_str()),
        Some("meta_out"),
        "reserved `channel` key must hold the real channel name"
    );
}

// ---------- 3.4: emit_with_meta on a pure inter-procedure channel ----------

fn pure_consumer_observed() -> &'static Mutex<Vec<u32>> {
    static C: OnceLock<Mutex<Vec<u32>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Vec::new()))
}

fn pure_emit_results() -> &'static Mutex<Vec<Result<(), String>>> {
    static C: OnceLock<Mutex<Vec<Result<(), String>>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Vec::new()))
}

test_procedure!(PureProducer, "hybrid_pure_producer", String => u32, (), |input, _state, ctx| {
    let n: u32 = input.parse().unwrap_or(0);
    let mut meta = HashMap::new();
    meta.insert("trace_id".to_string(), "abc123".to_string());
    let outcome = ctx
        .emit_with_meta::<u32>(n, meta)
        .map_err(|e| format!("{e}"));
    pure_emit_results().lock().unwrap().push(outcome);
    Ok(())
});

test_procedure!(PureConsumer, "hybrid_pure_consumer", u32 => (), (), |input, _state, _ctx| {
    pure_consumer_observed().lock().unwrap().push(input);
    Ok(())
});

struct PureOperative;
impl StreamOperative for PureOperative {
    fn factory_id() -> &'static str {
        "hybrid_pure"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<PureProducer>()
            .named("pure_out")
            .into::<PureConsumer>();
    }
}

#[test]
fn emit_with_meta_on_pure_inter_procedure_channel_is_rejected() {
    pure_consumer_observed().lock().unwrap().clear();
    pure_emit_results().lock().unwrap().clear();

    let (handle, _instance) = setup::<PureOperative>("hyb-pure");
    let (stream, received) = duplex_stream_with_reader();

    dispatch_input(&handle, "hyb-pure", &stream, "1");

    assert!(
        wait_until(
            || pure_emit_results().lock().unwrap().len() == 1,
            Duration::from_secs(5)
        ),
        "producer Procedure::run did not finish"
    );

    let outcomes = pure_emit_results().lock().unwrap().clone();
    let msg = outcomes[0].as_ref().expect_err("emit_with_meta must error");
    assert!(
        msg.contains("metadata is only valid") && msg.contains("pure_out"),
        "expected MetadataNotAllowedOnInterProcedureChannel; got: {msg}"
    );

    // No frame on the wire, no consumer delivery (producer returned the
    // error before fanning out).
    assert!(
        pure_consumer_observed().lock().unwrap().is_empty(),
        "consumer must not see the value when emit_with_meta rejects"
    );
    assert!(
        stays_quiet(
            || !received.lock().unwrap().is_empty(),
            Duration::from_millis(50)
        ),
        "no plugin frame should have been written"
    );
}

// ---------- one `sink`: terminal, sealed and hybrid are one declaration ----------

fn unified_consumer_observed() -> &'static Mutex<Vec<String>> {
    static C: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Vec::new()))
}

test_procedure!(UnifiedProducer, "unified_producer", String => String, (), |input, _state, ctx| {
    ctx.emit::<String>(input)
});

test_procedure!(UnifiedConsumer, "unified_consumer", String => (), (), |input, _state, _ctx| {
    unified_consumer_observed().lock().unwrap().push(input);
    Ok(())
});

/// `sink` and nothing after it — what the removed `output(...)` declared.
struct TerminalSinkOperative;
impl StreamOperative for TerminalSinkOperative {
    fn factory_id() -> &'static str {
        "unified_terminal"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<UnifiedProducer>()
            .named("unified_out")
            .sink("plugin_u");
    }
}

/// The same declaration with the optional type-level terminator.
struct SealedSinkOperative;
impl StreamOperative for SealedSinkOperative {
    fn factory_id() -> &'static str {
        "unified_sealed"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<UnifiedProducer>()
            .named("unified_out")
            .sink("plugin_u")
            .sealed();
    }
}

/// The same sink with a consumer wired after it — what `also_output` declared.
struct HybridSinkOperative;
impl StreamOperative for HybridSinkOperative {
    fn factory_id() -> &'static str {
        "unified_hybrid"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<UnifiedProducer>()
            .named("unified_out")
            .sink("plugin_u")
            .into::<UnifiedConsumer>();
    }
}

/// A leaf node sealed with no sink at all — the case where "sealing changes
/// no topology" is falsifiable: a leaf's output channel is created lazily, so
/// a `sealed()` that reached the builder would conjure one.
struct SealedLeafOperative;
impl StreamOperative for SealedLeafOperative {
    fn factory_id() -> &'static str {
        "unified_sealed_leaf"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<UnifiedProducer>()
            .sealed();
    }
}

/// The same graph with the `sealed()` call removed.
struct BareLeafOperative;
impl StreamOperative for BareLeafOperative {
    fn factory_id() -> &'static str {
        "unified_bare_leaf"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<UnifiedProducer>();
    }
}

/// Every channel in the topology, in declaration order.
fn channel_names<R: StreamOperative>() -> Vec<String> {
    build_topology_for::<R>()
        .expect("topology")
        .channels
        .iter()
        .map(|c| c.name.to_string())
        .collect()
}

/// fails when: `sealed()` reaches the builder — a leaf node would then carry
/// the phantom output channel the lazy creation exists to avoid, and the
/// "purely type-level" contract would be false wherever no sink precedes it.
#[test]
fn sealing_a_leaf_declares_no_channel() {
    assert_eq!(
        channel_names::<SealedLeafOperative>(),
        channel_names::<BareLeafOperative>()
    );
}

/// Everything about one channel the sink declaration decides.
fn sink_shape<R: StreamOperative>(channel: &str) -> (ChannelKind, usize, Option<String>, bool) {
    let topology = build_topology_for::<R>().expect("topology");
    let cid = topology.lookup_channel_id(channel).expect("channel id");
    let descriptor = topology.channel(cid).expect("descriptor");
    (
        descriptor.kind,
        descriptor.consumers.len(),
        descriptor.plugin_sink.as_ref().map(|s| s.name.to_string()),
        descriptor.keyed,
    )
}

/// Terminality is the absence of consumers, not a second method: the same
/// `.sink(...)` call yields the plugin-bound terminal or the hybrid channel
/// depending only on what is wired after it — and `.sealed()` is a statement
/// in the type system that changes neither.
#[test]
fn terminality_is_derived_from_the_wiring_and_sealed_changes_nothing() {
    let terminal = sink_shape::<TerminalSinkOperative>("unified_out");
    let sealed = sink_shape::<SealedSinkOperative>("unified_out");
    let hybrid = sink_shape::<HybridSinkOperative>("unified_out");

    assert_eq!(
        terminal, sealed,
        "`.sealed()` must add no topology difference"
    );
    assert_eq!(
        terminal,
        (
            ChannelKind::PluginBound,
            0,
            Some("plugin_u".to_string()),
            false
        ),
        "a sink with no consumers is the plugin-bound terminal"
    );
    assert_eq!(
        hybrid,
        (
            ChannelKind::InterProcedure,
            1,
            Some("plugin_u".to_string()),
            false
        ),
        "the same sink with a consumer after it stays inter-procedure"
    );
}

/// Wire fixture, held against the bytes the removed `output(...)` wrote:
/// default serialization (UTF-8 for `String`), the channel name under
/// `channel`, and no other metadata. Both shapes of the one `sink` method are
/// held against the same literals, so neither can drift from the other or
/// from what the rename replaced.
#[test]
fn terminal_and_hybrid_sink_frames_match_the_pre_rename_fixture() {
    let expected_meta: HashMap<String, String> =
        HashMap::from([("channel".to_string(), "unified_out".to_string())]);

    unified_consumer_observed().lock().unwrap().clear();

    let (terminal_handle, _t) = setup::<TerminalSinkOperative>("uni-terminal");
    let (terminal_stream, terminal_frames) = duplex_stream_with_reader();
    dispatch_input(&terminal_handle, "uni-terminal", &terminal_stream, "hello");

    let (hybrid_handle, _h) = setup::<HybridSinkOperative>("uni-hybrid");
    let (hybrid_stream, hybrid_frames) = duplex_stream_with_reader();
    dispatch_input(&hybrid_handle, "uni-hybrid", &hybrid_stream, "hello");

    assert!(
        wait_until(
            || terminal_frames.lock().unwrap().len() == 1
                && hybrid_frames.lock().unwrap().len() == 1
                && unified_consumer_observed().lock().unwrap().len() == 1,
            Duration::from_secs(5)
        ),
        "plugin frames or the hybrid consumer delivery did not arrive"
    );

    let terminal = terminal_frames.lock().unwrap();
    let (_, terminal_body, terminal_meta) = terminal.first().unwrap();
    assert_eq!(terminal_body, b"hello");
    assert_eq!(terminal_meta, &expected_meta);

    let hybrid = hybrid_frames.lock().unwrap();
    let (_, hybrid_body, hybrid_meta) = hybrid.first().unwrap();
    assert_eq!(hybrid_body, terminal_body);
    assert_eq!(hybrid_meta, terminal_meta);

    assert_eq!(
        *unified_consumer_observed().lock().unwrap(),
        vec!["hello".to_string()],
        "the hybrid consumer still receives the typed value"
    );
}

// Suppress unused-import lint if std::io::Read is only needed transitively.
#[allow(dead_code)]
fn _unused() {
    let _ = std::io::empty().bytes();
}
