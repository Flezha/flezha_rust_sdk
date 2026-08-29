//! Per-entity ordering and stickiness: one Entity is never dispatched
//! concurrently, its inputs keep enqueue order, different entities run in
//! parallel, and two operatives' same-named channels stay independent.

mod common;
use common::prelude::*;

// ----- Per-entity ordering (single mode) -----

static APPEND_OBSERVED: Mutex<Vec<String>> = Mutex::new(Vec::new());

test_procedure!(AppendProcedure, "append_procedure", String => String, (), |input, _state, _ctx| {
    APPEND_OBSERVED.lock().unwrap().push(input);
    Ok(())
});

struct AppendOperative;
impl StreamOperative for AppendOperative {
    fn factory_id() -> &'static str {
        "append_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<AppendProcedure>();
    }
}

#[test]
fn per_entity_input_order_preserved() {
    APPEND_OBSERVED.lock().unwrap().clear();
    let (handle, _instance) = setup::<AppendOperative>("append-instance");
    let stream = dummy_stream();

    let n = 1000usize;
    for i in 0..n {
        dispatch_input(
            &handle,
            "append-instance",
            "input",
            format!("{}", i).into_bytes(),
            &stream,
        );
    }

    assert!(
        wait_until(
            || APPEND_OBSERVED.lock().unwrap().len() == n,
            Duration::from_secs(5)
        ),
        "drain did not complete: {} of {}",
        APPEND_OBSERVED.lock().unwrap().len(),
        n
    );

    let observed = APPEND_OBSERVED.lock().unwrap().clone();
    for (i, s) in observed.iter().enumerate() {
        assert_eq!(*s, format!("{}", i), "out-of-order at {}", i);
    }
}

// ----- Sticky non-concurrent dispatch on the same entity -----

static APPEND_CONCURRENT: AtomicUsize = AtomicUsize::new(0);
static APPEND_MAX_CONCURRENT: AtomicUsize = AtomicUsize::new(0);

test_procedure!(ConcurrencyProbeProcedure, "concurrency_probe_procedure", String => String, (), |_input, _state, _ctx| {
    let cur = APPEND_CONCURRENT.fetch_add(1, Ordering::AcqRel) + 1;
    let mut prev = APPEND_MAX_CONCURRENT.load(Ordering::Acquire);
    while cur > prev {
        match APPEND_MAX_CONCURRENT.compare_exchange_weak(
            prev,
            cur,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => break,
            Err(p) => prev = p,
        }
    }
    for _ in 0..50 {
        std::hint::spin_loop();
    }
    APPEND_CONCURRENT.fetch_sub(1, Ordering::AcqRel);
    Ok(())
});

struct ConcurrencyProbeOperative;
impl StreamOperative for ConcurrencyProbeOperative {
    fn factory_id() -> &'static str {
        "concurrency_probe_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<ConcurrencyProbeProcedure>();
    }
}

#[test]
fn same_entity_never_runs_concurrently() {
    APPEND_CONCURRENT.store(0, Ordering::Release);
    APPEND_MAX_CONCURRENT.store(0, Ordering::Release);

    let topology = build_topology_for::<ConcurrencyProbeOperative>().expect("topology");
    let registry = Arc::new(OperativeRegistry::new());
    let instance = StreamInstance::new(topology, Arc::from("conc-instance"));
    registry.register_stream(instance);
    let handle = OperativeDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = dummy_stream();

    let total = 2_000usize;
    let producers = 4usize;
    let mut threads = Vec::new();
    for _ in 0..producers {
        let handle = handle.clone();
        let stream = Arc::clone(&stream);
        threads.push(std::thread::spawn(move || {
            for i in 0..(total / producers) {
                let mut md = HashMap::new();
                md.insert("channel".to_string(), "input".to_string());
                handle.dispatch(
                    "conc-instance",
                    format!("{}", i).into_bytes(),
                    &md,
                    Arc::clone(&stream),
                );
            }
        }));
    }
    for t in threads {
        t.join().unwrap();
    }

    // Wait for drain: nothing running and nothing queued.
    let slot = Arc::clone(&registry_instance(&registry, "conc-instance").entities[0][0]);
    assert!(wait_until(
        || APPEND_CONCURRENT.load(Ordering::Acquire) == 0 && slot.mailbox.is_empty(),
        Duration::from_secs(10)
    ));

    assert_eq!(
        APPEND_MAX_CONCURRENT.load(Ordering::Acquire),
        1,
        "single-entity mailbox allowed concurrent dispatch"
    );
}

fn registry_instance(registry: &Arc<OperativeRegistry>, id: &str) -> Arc<StreamInstance> {
    registry.get_stream(id).expect("registered instance")
}

// ----- Parallel entities of the same procedure -----

use std::sync::OnceLock;

fn parallel_observed() -> &'static Mutex<HashMap<&'static str, Vec<String>>> {
    static CELL: OnceLock<Mutex<HashMap<&'static str, Vec<String>>>> = OnceLock::new();
    CELL.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Default)]
struct ParallelStaticProcedure;
impl Procedure for ParallelStaticProcedure {
    type Input = String;
    type Output = String;
    type State = String;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "parallel_static_procedure"
    }
    fn init_state(
        entity_key: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> String {
        match entity_key {
            Some("a") => "a".to_string(),
            Some("b") => "b".to_string(),
            Some("c") => "c".to_string(),
            Some("d") => "d".to_string(),
            _ => "?".to_string(),
        }
    }
    fn entity_keys() -> Vec<&'static str> {
        vec!["a", "b", "c", "d"]
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(input: &String) -> Vec<&'static str> {
        match input.chars().next() {
            Some('a') => vec!["a"],
            Some('b') => vec!["b"],
            Some('c') => vec!["c"],
            _ => vec!["d"],
        }
    }
    fn run(
        &mut self,
        input: String,
        state: StateHandle<'_, ParallelStaticProcedure>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let key: &'static str = match state.as_str() {
            "a" => "a",
            "b" => "b",
            "c" => "c",
            "d" => "d",
            _ => "?",
        };
        parallel_observed()
            .lock()
            .unwrap()
            .entry(key)
            .or_default()
            .push(input);
        Ok(())
    }
}

struct ParallelOperative;
impl StreamOperative for ParallelOperative {
    fn factory_id() -> &'static str {
        "parallel_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<ParallelStaticProcedure>();
    }
}

#[test]
fn static_entities_dispatch_independently() {
    parallel_observed().lock().unwrap().clear();
    let (handle, _instance) = setup::<ParallelOperative>("parallel-instance");
    let stream = dummy_stream();

    let prefixes = ["a", "b", "c", "d"];
    let per_prefix = 100usize;
    for &p in &prefixes {
        for i in 0..per_prefix {
            dispatch_input(
                &handle,
                "parallel-instance",
                "input",
                format!("{}{}", p, i).into_bytes(),
                &stream,
            );
        }
    }

    assert!(wait_until(
        || {
            let map = parallel_observed().lock().unwrap();
            prefixes
                .iter()
                .all(|k| map.get(*k).map(|v| v.len() == per_prefix).unwrap_or(false))
        },
        Duration::from_secs(5)
    ));

    let map = parallel_observed().lock().unwrap();
    for &p in &prefixes {
        let v = map.get(p).expect("entity observed");
        assert_eq!(v.len(), per_prefix);
        for (i, s) in v.iter().enumerate() {
            assert_eq!(*s, format!("{}{}", p, i), "{} entity out of order", p);
        }
    }
}

// ----- Per-operative namespace isolation -----

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct CountingState {
    n: usize,
}

test_procedure!(CountingProcedure, "counting_procedure", String => String, CountingState, |_input, _state, _ctx| {
    Ok(())
});

struct StreamA;
impl StreamOperative for StreamA {
    fn factory_id() -> &'static str {
        "ns_operative_a"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<CountingProcedure>();
    }
}

struct StreamB;
impl StreamOperative for StreamB {
    fn factory_id() -> &'static str {
        "ns_operative_b"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<CountingProcedure>();
    }
}

#[test]
fn per_operative_namespace_isolation() {
    let topology_a = build_topology_for::<StreamA>().expect("topology a");
    let topology_b = build_topology_for::<StreamB>().expect("topology b");
    assert_ne!(topology_a.factory_id, topology_b.factory_id);

    // Same channel name on each is a separate ChannelDescriptor with id 0
    // in its own topology — interning is per-operative.
    assert_eq!(topology_a.lookup_channel_id("input"), Some(0));
    assert_eq!(topology_b.lookup_channel_id("input"), Some(0));
    assert!(!std::ptr::eq(
        Arc::as_ptr(&topology_a),
        Arc::as_ptr(&topology_b),
    ));
}

// ----- emit_with_meta rejection on inter-procedure channels -----

static META_REJECT_OBSERVED: Mutex<Option<String>> = Mutex::new(None);

test_procedure!(MetaProducerProcedure, "meta_producer_procedure", String => String, (), |input, _state, ctx| {
    let mut meta = HashMap::new();
    meta.insert("k".to_string(), "v".to_string());
    let res = ctx.emit_with_meta::<String>(input, meta);
    if let Err(err) = res {
        *META_REJECT_OBSERVED.lock().unwrap() = Some(err.to_string());
    }
    Ok(())
});

test_procedure!(MetaConsumerProcedure, "meta_consumer_procedure", String => (), (), |_input, _state, _ctx| {
    Ok(())
});

struct MetaRejectOperative;
impl StreamOperative for MetaRejectOperative {
    fn factory_id() -> &'static str {
        "meta_reject_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<MetaProducerProcedure>()
            .named("hop")
            .into::<MetaConsumerProcedure>();
    }
}

#[test]
fn emit_with_meta_rejected_on_inter_procedure() {
    *META_REJECT_OBSERVED.lock().unwrap() = None;
    let (handle, _instance) = setup::<MetaRejectOperative>("meta-instance");
    let stream = dummy_stream();

    let mut md = HashMap::new();
    md.insert("channel".to_string(), "input".to_string());
    handle.dispatch("meta-instance", b"hello".to_vec(), &md, Arc::clone(&stream));

    assert!(wait_until(
        || META_REJECT_OBSERVED.lock().unwrap().is_some(),
        Duration::from_secs(2)
    ));
    let msg = META_REJECT_OBSERVED.lock().unwrap().clone().unwrap();
    assert!(
        msg.contains("inter-procedure"),
        "expected inter-procedure rejection, got {msg}"
    );
}
