//! `operative-dynamic-segmenting` runtime tests: lazy create, soft cap,
//! no-key drop (§2); idle-TTL eviction and the CAS-vs-reaper race (§3);
//! concurrent-producer + cold-recreate (§4).
//!
//! The dynamic key is the substring before the first `:`; the literal
//! `"drop"` yields `None` (exercises the no-key-drop path).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

use flezha_executor::channel::ChannelBuilder;
use flezha_executor::segment::dynamic::DynamicSegmentRegistry;
use flezha_executor::operative::ContinuousInstance;
use flezha_executor::operation::{Operation, OperationCtx, OperationError};
use flezha_executor::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_executor::ContinuousOperative;

mod common;
use common::{dispatch_input, dummy_stream, setup, wait_until};

fn key_of(input: &str) -> Option<String> {
    (input != "drop").then(|| input.split(':').next().unwrap_or(input).to_string())
}

/// A `String`-in, `()`-state dynamic-mode Operation keyed by [`key_of`]. The
/// base form takes the `init_state` + `run` bodies; `dyn_operation!` adds
/// trivial ones plus a paired single-entry ContinuousOperative. Extra trait items (TTL,
/// cap) are appended verbatim.
macro_rules! dyn_operation_base {
    ($operation:ident, $operative:ident, $name:literal, $factory:literal, { $($item:item)* }) => {
        #[derive(Default)]
        struct $operation;
        impl Operation for $operation {
            type Input = String;
            type Output = ();
            type State = ();
            type Change = NoChange;
            type Clock = WallClock;
            type Config = ();
            fn name() -> &'static str { $name }
            const OPERATION_KEY_OVERRIDDEN: bool = true;
            fn operation_key(input: &String) -> Option<String> { key_of(input) }
            $($item)*
        }
        struct $operative;
        impl ContinuousOperative for $operative {
            fn factory_id() -> &'static str { $factory }
            fn channels(c: &mut ChannelBuilder) { c.input::<String>("inp").into::<$operation>(); }
        }
    };
}

macro_rules! dyn_operation {
    ($operation:ident, $operative:ident, $name:literal, $factory:literal $($extra:item)*) => {
        dyn_operation_base!($operation, $operative, $name, $factory, {
            fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
            fn run(&mut self, _: String, _: StateHandle<'_, Self>, _: &mut OperationCtx<'_>)
                -> Result<(), OperationError> { Ok(()) }
            $($extra)*
        });
    };
}

dyn_operation!(DynOperation, DynOperative, "dyn_operation", "dyn_operative");
dyn_operation!(CappedOperation, CappedOperative, "capped_operation", "capped_operative"
    fn max_segments() -> usize { 2 }
    const MAX_SEGMENTS_OVERRIDDEN: bool = true;
);
dyn_operation!(TtlOperation, TtlOperative, "ttl_operation", "ttl_operative"
    fn segment_idle_ttl() -> Option<Duration> { Some(Duration::from_millis(100)) }
);

static SLEEP_RUN_STARTED: AtomicBool = AtomicBool::new(false);
static SLEEP_RUN_DONE: AtomicBool = AtomicBool::new(false);
dyn_operation_base!(SleepOperation, SleepOperative, "sleep_operation", "sleep_operative", {
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(&mut self, _: String, _: StateHandle<'_, Self>, _: &mut OperationCtx<'_>) -> Result<(), OperationError> {
        SLEEP_RUN_STARTED.store(true, Ordering::Release);
        thread::sleep(Duration::from_millis(500));
        SLEEP_RUN_DONE.store(true, Ordering::Release);
        Ok(())
    }
});

static INIT_KEYS: Mutex<Vec<String>> = Mutex::new(Vec::new());
dyn_operation_base!(InitTrackOperation, InitTrackOperative, "init_track_operation", "init_track_operative", {
    fn init_state(key: Option<&str>, _: Option<RestoreInitPayload<'_>>) {
        if let Some(k) = key {
            INIT_KEYS.lock().unwrap().push(k.to_string());
        }
    }
    fn run(&mut self, _: String, _: StateHandle<'_, Self>, _: &mut OperationCtx<'_>) -> Result<(), OperationError> {
        Ok(())
    }
});

/// The single dynamic registry on a one-operation operative instance.
fn registry(instance: &Arc<ContinuousInstance>) -> Arc<DynamicSegmentRegistry> {
    Arc::clone(instance.dynamic_registries[0].as_ref().expect("operation 0 is dynamic"))
}

fn created(reg: &DynamicSegmentRegistry) -> u64 {
    reg.counters.segments_created_total.load(Ordering::Relaxed)
}
fn evicted(reg: &DynamicSegmentRegistry) -> u64 {
    reg.counters.segments_evicted_total.load(Ordering::Relaxed)
}

#[test]
fn lazy_create_two_keys() {
    let (handle, instance) = setup::<DynOperative>("dyn-2", 4);
    let stream = dummy_stream();
    dispatch_input(&handle, "dyn-2", "inp", b"a:1".to_vec(), &stream);
    dispatch_input(&handle, "dyn-2", "inp", b"b:2".to_vec(), &stream);

    let reg = registry(&instance);
    assert_eq!(reg.segments.len(), 2, "two distinct keys → two slots");
    assert_eq!(created(&reg), 2);
}

#[test]
fn soft_cap_drops_overflow_key() {
    let (handle, instance) = setup::<CappedOperative>("capped-1", 4);
    let stream = dummy_stream();
    for inp in [b"a:1", b"b:2", b"c:3"] {
        dispatch_input(&handle, "capped-1", "inp", inp.to_vec(), &stream);
    }

    let reg = registry(&instance);
    assert_eq!(reg.segments.len(), 2, "cap = 2 → at most two slots");
    assert_eq!(
        reg.counters.inputs_dropped_capped_total.load(Ordering::Relaxed),
        1,
        "the third distinct key is dropped"
    );
}

#[test]
fn none_key_inputs_are_dropped() {
    let (handle, instance) = setup::<DynOperative>("dyn-nokey", 4);
    let stream = dummy_stream();
    // 10 inputs; 4 "drop" sentinels → 6 slot creations.
    for inp in ["a:1", "drop", "b:2", "drop", "c:3", "drop", "d:4", "drop", "e:5", "f:6"] {
        dispatch_input(&handle, "dyn-nokey", "inp", inp.as_bytes().to_vec(), &stream);
    }

    let reg = registry(&instance);
    assert!(wait_until(|| reg.segments.len() == 6, Duration::from_secs(10)));
    assert_eq!(created(&reg), 6);
    assert_eq!(reg.counters.inputs_dropped_no_key_total.load(Ordering::Relaxed), 4);
}

#[test]
fn idle_segment_is_reaped() {
    // Reaper sweeps every 25ms; the 100ms-TTL Segment is evicted shortly
    // after it goes idle. Generous deadline absorbs full-suite scheduling
    // starvation (the reaper retries every sweep).
    std::env::set_var("FLEZHA_REAP_INTERVAL_MS", "25");
    let (handle, instance) = setup::<TtlOperative>("ttl-1", 4);
    let stream = dummy_stream();
    dispatch_input(&handle, "ttl-1", "inp", b"K:1".to_vec(), &stream);

    // Key off the eviction counter, not the map: the reaper removes the
    // entry a hair before bumping the counter, so a cross-thread watcher of
    // `segments` could otherwise win that window. Generous deadline
    // absorbs full-suite scheduling starvation (the reaper retries each sweep).
    let reg = registry(&instance);
    assert!(wait_until(|| evicted(&reg) == 1, Duration::from_secs(15)));
    assert!(reg.segments.is_empty());
}

#[test]
fn reaper_skips_segment_with_active_run() {
    SLEEP_RUN_STARTED.store(false, Ordering::Release);
    SLEEP_RUN_DONE.store(false, Ordering::Release);
    let (handle, instance) = setup::<SleepOperative>("sleep-1", 4);
    let stream = dummy_stream();
    dispatch_input(&handle, "sleep-1", "inp", b"K:1".to_vec(), &stream);
    let reg = registry(&instance);

    assert!(wait_until(|| SLEEP_RUN_STARTED.load(Ordering::Acquire), Duration::from_secs(10)));
    // The run holds the drain gate; a sweep (even at 1ms TTL) must not evict.
    reg.reap_sweep(Duration::from_millis(1));
    assert_eq!(reg.segments.len(), 1, "evicted while Operation::run was active");
    assert_eq!(evicted(&reg), 0);

    // Once the run releases the gate, the next sweep evicts.
    assert!(wait_until(|| SLEEP_RUN_DONE.load(Ordering::Acquire), Duration::from_secs(10)));
    assert!(wait_until(
        || {
            reg.reap_sweep(Duration::from_millis(1));
            reg.segments.is_empty()
        },
        Duration::from_secs(10)
    ));
    assert_eq!(evicted(&reg), 1);
}

#[test]
fn recreate_after_evict_cold_starts_fresh() {
    INIT_KEYS.lock().unwrap().clear();
    let (handle, instance) = setup::<InitTrackOperative>("recreate-1", 4);
    let stream = dummy_stream();
    let reg = registry(&instance);

    dispatch_input(&handle, "recreate-1", "inp", b"K:1".to_vec(), &stream);
    assert!(wait_until(|| reg.segments.len() == 1, Duration::from_secs(10)));

    assert!(wait_until(
        || {
            reg.reap_sweep(Duration::from_millis(1));
            reg.segments.is_empty()
        },
        Duration::from_secs(10)
    ));

    // Key returns → a brand-new slot + a SECOND init_state(Some("K")): a cold
    // recreate, not a restore.
    dispatch_input(&handle, "recreate-1", "inp", b"K:2".to_vec(), &stream);
    assert!(wait_until(|| reg.segments.len() == 1, Duration::from_secs(10)));

    assert_eq!(*INIT_KEYS.lock().unwrap(), vec!["K".to_string(), "K".to_string()]);
    assert_eq!(created(&reg), 2);
    assert_eq!(evicted(&reg), 1);
}

#[test]
fn concurrent_producers_share_one_slot() {
    let (handle, instance) = setup::<DynOperative>("dyn-concurrent", 8);
    let reg = registry(&instance);

    const N: usize = 16;
    let barrier = Arc::new(Barrier::new(N));
    let threads: Vec<_> = (0..N)
        .map(|i| {
            let handle = handle.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let stream = dummy_stream();
                barrier.wait();
                dispatch_input(&handle, "dyn-concurrent", "inp", format!("K:{i}").into_bytes(), &stream);
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }

    // DashMap serialized the inserts: one slot, created once, all 16 landed.
    assert_eq!(reg.segments.len(), 1);
    assert_eq!(created(&reg), 1);
    let slot = reg.segments.get("K").expect("slot for K");
    assert!(wait_until(
        || slot.counters().pushes_total.load(Ordering::Relaxed) == N as u64,
        Duration::from_secs(10)
    ));
}
