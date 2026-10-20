//! Per-cell ordering and stickiness: one Cell is never dispatched
//! concurrently, its inputs keep enqueue order, different cells run in
//! parallel, and two relays' same-named channels stay independent.

mod common;
use common::prelude::*;

// ----- Per-cell ordering (single mode) -----

static APPEND_OBSERVED: Mutex<Vec<String>> = Mutex::new(Vec::new());

test_tasklet!(AppendTasklet, "append_tasklet", String => String, (), |input, _ctx| {
    APPEND_OBSERVED.lock().unwrap().push(input);
    Ok(())
});

struct AppendRelay;
impl Relay for AppendRelay {
    fn factory_id() -> &'static str {
        "append_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<AppendTasklet>();
    }
}

#[test]
fn per_cell_input_order_preserved() {
    APPEND_OBSERVED.lock().unwrap().clear();
    let (handle, _instance) = setup::<AppendRelay>("append-instance");
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

// ----- Sticky non-concurrent dispatch on the same cell -----

static APPEND_CONCURRENT: AtomicUsize = AtomicUsize::new(0);
static APPEND_MAX_CONCURRENT: AtomicUsize = AtomicUsize::new(0);

test_tasklet!(ConcurrencyProbeTasklet, "concurrency_probe_tasklet", String => String, (), |_input, _ctx| {
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

struct ConcurrencyProbeRelay;
impl Relay for ConcurrencyProbeRelay {
    fn factory_id() -> &'static str {
        "concurrency_probe_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<ConcurrencyProbeTasklet>();
    }
}

#[test]
fn same_cell_never_runs_concurrently() {
    APPEND_CONCURRENT.store(0, Ordering::Release);
    APPEND_MAX_CONCURRENT.store(0, Ordering::Release);

    let topology = build_topology_for::<ConcurrencyProbeRelay>().expect("topology");
    let registry = Arc::new(RelayRegistry::new());
    let instance = RelayInstance::new(topology, Arc::from("conc-instance"));
    registry.register(instance);
    let handle = RelayDispatchHandle {
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
    let slot = Arc::clone(&registry_instance(&registry, "conc-instance").cells[0][0]);
    assert!(wait_until(
        || APPEND_CONCURRENT.load(Ordering::Acquire) == 0 && slot.mailbox.is_empty(),
        Duration::from_secs(10)
    ));

    assert_eq!(
        APPEND_MAX_CONCURRENT.load(Ordering::Acquire),
        1,
        "single-cell mailbox allowed concurrent dispatch"
    );
}

fn registry_instance(registry: &Arc<RelayRegistry>, id: &str) -> Arc<RelayInstance> {
    registry.get(id).expect("registered instance")
}

// ----- Parallel cells of the same tasklet -----

use std::sync::OnceLock;

fn parallel_observed() -> &'static Mutex<HashMap<&'static str, Vec<String>>> {
    static OBSERVED: OnceLock<Mutex<HashMap<&'static str, Vec<String>>>> = OnceLock::new();
    OBSERVED.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Default)]
struct ParallelStaticTasklet;
#[flezha_runner::tasklet(name = "parallel_static_tasklet", cell_keys = ["a", "b", "c", "d"])]
impl Tasklet for ParallelStaticTasklet {
    type Input = String;
    type Output = String;
    type State = String;
    fn init_state(cell_key: Option<&str>) -> String {
        match cell_key {
            Some("a") => "a".to_string(),
            Some("b") => "b".to_string(),
            Some("c") => "c".to_string(),
            Some("d") => "d".to_string(),
            _ => "?".to_string(),
        }
    }
    fn cells_for(input: &String) -> Vec<&'static str> {
        match input.chars().next() {
            Some('a') => vec!["a"],
            Some('b') => vec!["b"],
            Some('c') => vec!["c"],
            _ => vec!["d"],
        }
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let key: &'static str = match ctx.state().as_str() {
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

struct ParallelRelay;
impl Relay for ParallelRelay {
    fn factory_id() -> &'static str {
        "parallel_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<ParallelStaticTasklet>();
    }
}

#[test]
fn static_cells_dispatch_independently() {
    parallel_observed().lock().unwrap().clear();
    let (handle, _instance) = setup::<ParallelRelay>("parallel-instance");
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
        let v = map.get(p).expect("cell observed");
        assert_eq!(v.len(), per_prefix);
        for (i, s) in v.iter().enumerate() {
            assert_eq!(*s, format!("{}{}", p, i), "{} cell out of order", p);
        }
    }
}

// ----- Per-relay namespace isolation -----

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct CountingState {
    n: usize,
}

test_tasklet!(CountingTasklet, "counting_tasklet", String => String, CountingState, |_input, _ctx| {
    Ok(())
});

struct FlowA;
impl Relay for FlowA {
    fn factory_id() -> &'static str {
        "ns_relay_a"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<CountingTasklet>();
    }
}

struct FlowB;
impl Relay for FlowB {
    fn factory_id() -> &'static str {
        "ns_relay_b"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<CountingTasklet>();
    }
}

#[test]
fn per_relay_namespace_isolation() {
    let topology_a = build_topology_for::<FlowA>().expect("topology a");
    let topology_b = build_topology_for::<FlowB>().expect("topology b");
    assert_ne!(topology_a.factory_id, topology_b.factory_id);

    // Same channel name on each is a separate ChannelDescriptor with id 0
    // in its own topology — interning is per-relay.
    assert_eq!(topology_a.lookup_channel_id("input"), Some(0));
    assert_eq!(topology_b.lookup_channel_id("input"), Some(0));
    assert!(!std::ptr::eq(
        Arc::as_ptr(&topology_a),
        Arc::as_ptr(&topology_b),
    ));
}

// ----- emit_with_meta rejection on inter-tasklet channels -----

static META_REJECT_OBSERVED: Mutex<Option<String>> = Mutex::new(None);

test_tasklet!(MetaProducerTasklet, "meta_producer_tasklet", String => String, (), |input, ctx| {
    let mut meta = HashMap::new();
    meta.insert("k".to_string(), "v".to_string());
    let res = ctx.emit_with_meta(input, meta);
    if let Err(err) = res {
        *META_REJECT_OBSERVED.lock().unwrap() = Some(err.to_string());
    }
    Ok(())
});

test_tasklet!(MetaConsumerTasklet, "meta_consumer_tasklet", String => (), (), |_input, _ctx| {
    Ok(())
});

struct MetaRejectRelay;
impl Relay for MetaRejectRelay {
    fn factory_id() -> &'static str {
        "meta_reject_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<MetaProducerTasklet>()
            .named("hop")
            .into::<MetaConsumerTasklet>();
    }
}

#[test]
fn emit_with_meta_rejected_on_inter_tasklet() {
    *META_REJECT_OBSERVED.lock().unwrap() = None;
    let (handle, _instance) = setup::<MetaRejectRelay>("meta-instance");
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
        msg.contains("between two tasklets"),
        "expected inter-tasklet rejection, got {msg}"
    );
}
