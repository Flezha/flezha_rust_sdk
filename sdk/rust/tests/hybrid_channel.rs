//! Section 2 unit tests for hybrid channels (`also_to_plugin_with`).
//!
//! Each test uses the live dispatcher: an entry channel feeds bytes
//! into a producer Operation, which calls `ctx.emit(typed_value)` on a
//! channel wired to one or more inter-operation consumers and a plugin
//! sink. The consumer-side delivery is observed via static state in
//! the consumer Operation; the plugin-side delivery is observed by reading
//! the receiving end of the unix socket pair and parsing wire frames.

use std::collections::HashMap;
use std::io::Read;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use bi::OperativeDispatchHandle;
use flezha_executor::channel::ChannelBuilder;
use flezha_executor::operative::builder::build_topology_for;
use flezha_executor::{__dispatch_internals as bi, ContinuousOperative};
use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;

// ---------- shared scaffolding ----------

mod common;
use common::{setup, wait_until};

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

test_operation!(SingleProducer, "hybrid_single_producer", String => u32, (), |input, _state, ctx| {
    let n: u32 = input.parse().unwrap_or(0);
    ctx.emit::<u32>(n)
});

test_operation!(SingleConsumer, "hybrid_single_consumer", u32 => (), (), |input, _state, _ctx| {
    single_consumer_observed().lock().unwrap().push(input);
    Ok(())
});

struct SingleHybridRelay;
impl ContinuousOperative for SingleHybridRelay {
    fn factory_id() -> &'static str {
        "hybrid_single"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<SingleProducer>();
        c.channel::<u32>("hybrid_out")
            .from::<SingleProducer>()
            .also_to_plugin_with("plugin_a", |v: &u32| v.to_be_bytes().to_vec())
            .into::<SingleConsumer>();
    }
}

#[test]
fn hybrid_single_consumer_receives_typed_and_plugin_bytes() {
    single_consumer_observed().lock().unwrap().clear();
    let (handle, _instance) = setup::<SingleHybridRelay>("hyb-single", 4);
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

test_operation!(ThreeProducer, "hybrid_three_producer", String => u32, (), |input, _state, ctx| {
    let n: u32 = input.parse().unwrap_or(0);
    ctx.emit::<u32>(n)
});

test_operation!(ThreeConsumerA, "hybrid_three_a", u32 => (), (), |input, _state, _ctx| {
    three_consumer_observed_a().lock().unwrap().push(input);
    Ok(())
});
test_operation!(ThreeConsumerB, "hybrid_three_b", u32 => (), (), |input, _state, _ctx| {
    three_consumer_observed_b().lock().unwrap().push(input);
    Ok(())
});
test_operation!(ThreeConsumerC, "hybrid_three_c", u32 => (), (), |input, _state, _ctx| {
    three_consumer_observed_c().lock().unwrap().push(input);
    Ok(())
});

struct ThreeHybridRelay;
impl ContinuousOperative for ThreeHybridRelay {
    fn factory_id() -> &'static str {
        "hybrid_three"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<ThreeProducer>();
        c.channel::<u32>("three_out")
            .from::<ThreeProducer>()
            .also_to_plugin_with("plugin_three", |v: &u32| {
                SERIALIZER_CALLS.fetch_add(1, Ordering::SeqCst);
                v.to_be_bytes().to_vec()
            })
            .into::<ThreeConsumerA>()
            .into::<ThreeConsumerB>()
            .into::<ThreeConsumerC>();
    }
}

#[test]
fn hybrid_three_consumers_single_serializer_call() {
    three_consumer_observed_a().lock().unwrap().clear();
    three_consumer_observed_b().lock().unwrap().clear();
    three_consumer_observed_c().lock().unwrap().clear();
    SERIALIZER_CALLS.store(0, Ordering::SeqCst);

    let (handle, _instance) = setup::<ThreeHybridRelay>("hyb-three", 4);
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

test_operation!(PanicProducer, "hybrid_panic_producer", String => u32, (), |input, _state, ctx| {
    let n: u32 = input.parse().unwrap_or(0);
    let outcome = ctx.emit::<u32>(n).map_err(|e| format!("{e}"));
    panic_emit_results().lock().unwrap().push(outcome);
    Ok(())
});

test_operation!(PanicConsumer, "hybrid_panic_consumer", u32 => (), (), |input, _state, _ctx| {
    panic_consumer_observed().lock().unwrap().push(input);
    Ok(())
});

struct PanicHybridRelay;
impl ContinuousOperative for PanicHybridRelay {
    fn factory_id() -> &'static str {
        "hybrid_panic"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<PanicProducer>();
        c.channel::<u32>("panic_out")
            .from::<PanicProducer>()
            .also_to_plugin_with("plugin_panic", |v: &u32| {
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

    let (handle, _instance) = setup::<PanicHybridRelay>("hyb-panic", 4);
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

// ---------- 2.8: duplicate also_to_plugin_with rejected at registration ----------

test_operation!(DupProducer, "hybrid_dup_producer", String => u32, (), |_input, _state, _ctx| {
    Ok(())
});

struct DupHybridRelay;
impl ContinuousOperative for DupHybridRelay {
    fn factory_id() -> &'static str {
        "hybrid_dup"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<DupProducer>();
        c.channel::<u32>("dup_out")
            .from::<DupProducer>()
            .also_to_plugin_with("plugin_a", |v: &u32| v.to_be_bytes().to_vec())
            .also_to_plugin_with("plugin_b", |v: &u32| v.to_be_bytes().to_vec());
    }
}

#[test]
fn hybrid_duplicate_plugin_sink_is_rejected() {
    let result = build_topology_for::<DupHybridRelay>();
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

test_operation!(MetaProducer, "hybrid_meta_producer", String => u32, (), |input, _state, ctx| {
    let n: u32 = input.parse().unwrap_or(0);
    let mut meta = HashMap::new();
    meta.insert("trace_id".to_string(), "abc123".to_string());
    // Reserved key: must be silently dropped from user-supplied
    // metadata and replaced by the real channel name.
    meta.insert("channel".to_string(), "spoof".to_string());
    ctx.emit_with_meta::<u32>(n, meta)
});

test_operation!(MetaConsumer, "hybrid_meta_consumer", u32 => (), (), |input, _state, _ctx| {
    meta_consumer_observed().lock().unwrap().push(input);
    Ok(())
});

struct MetaHybridRelay;
impl ContinuousOperative for MetaHybridRelay {
    fn factory_id() -> &'static str {
        "hybrid_meta"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<MetaProducer>();
        c.channel::<u32>("meta_out")
            .from::<MetaProducer>()
            .also_to_plugin_with("plugin_meta", |v: &u32| v.to_be_bytes().to_vec())
            .into::<MetaConsumer>();
    }
}

#[test]
fn emit_with_meta_on_hybrid_channel_forwards_meta_to_plugin_only() {
    meta_consumer_observed().lock().unwrap().clear();
    let (handle, _instance) = setup::<MetaHybridRelay>("hyb-meta", 4);
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

// ---------- 3.4: emit_with_meta on a pure inter-operation channel ----------

fn pure_consumer_observed() -> &'static Mutex<Vec<u32>> {
    static C: OnceLock<Mutex<Vec<u32>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Vec::new()))
}

fn pure_emit_results() -> &'static Mutex<Vec<Result<(), String>>> {
    static C: OnceLock<Mutex<Vec<Result<(), String>>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Vec::new()))
}

test_operation!(PureProducer, "hybrid_pure_producer", String => u32, (), |input, _state, ctx| {
    let n: u32 = input.parse().unwrap_or(0);
    let mut meta = HashMap::new();
    meta.insert("trace_id".to_string(), "abc123".to_string());
    let outcome = ctx
        .emit_with_meta::<u32>(n, meta)
        .map_err(|e| format!("{e}"));
    pure_emit_results().lock().unwrap().push(outcome);
    Ok(())
});

test_operation!(PureConsumer, "hybrid_pure_consumer", u32 => (), (), |input, _state, _ctx| {
    pure_consumer_observed().lock().unwrap().push(input);
    Ok(())
});

struct PureRelay;
impl ContinuousOperative for PureRelay {
    fn factory_id() -> &'static str {
        "hybrid_pure"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<PureProducer>();
        c.channel::<u32>("pure_out")
            .from::<PureProducer>()
            .into::<PureConsumer>();
    }
}

#[test]
fn emit_with_meta_on_pure_inter_operation_channel_is_rejected() {
    pure_consumer_observed().lock().unwrap().clear();
    pure_emit_results().lock().unwrap().clear();

    let (handle, _instance) = setup::<PureRelay>("hyb-pure", 4);
    let (stream, received) = duplex_stream_with_reader();

    dispatch_input(&handle, "hyb-pure", &stream, "1");

    assert!(
        wait_until(
            || pure_emit_results().lock().unwrap().len() == 1,
            Duration::from_secs(5)
        ),
        "producer Operation::run did not finish"
    );

    let outcomes = pure_emit_results().lock().unwrap().clone();
    let msg = outcomes[0].as_ref().expect_err("emit_with_meta must error");
    assert!(
        msg.contains("metadata is only valid") && msg.contains("pure_out"),
        "expected MetadataNotAllowedOnInterOperationChannel; got: {msg}"
    );

    // No frame on the wire, no consumer delivery (producer returned the
    // error before fanning out).
    assert!(
        pure_consumer_observed().lock().unwrap().is_empty(),
        "consumer must not see the value when emit_with_meta rejects"
    );
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        received.lock().unwrap().is_empty(),
        "no plugin frame should have been written"
    );
}

// Suppress unused-import lint if std::io::Read is only needed transitively.
#[allow(dead_code)]
fn _unused() {
    let _ = std::io::empty().bytes();
}
