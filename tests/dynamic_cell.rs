//! dynamic cell mode runtime tests: lazy create, soft cap,
//! filtered-out inputs (§2); idle-TTL eviction and the CAS-vs-reaper race
//! (§3); concurrent-producer + cold-recreate (§4).
//!
//! The dynamic key is declared on the WIRING: the substring before the first
//! `:`. There is no keyless-drop path — the literal `"drop"` is removed by a
//! `filter` declared before the keying, which is how an input is discarded.

use std::sync::{Arc, Barrier, Mutex};
use std::thread;

use flezha_runner::cell::dynamic::DynamicCellRegistry;
use flezha_runner::cell::flusher::SendFrameFn;
use flezha_runner::cell::state_sink::{self, install_global_sink};
use flz_transport::service::payloader;

mod common;
use common::prelude::*;
use common::{dispatch_input, dummy_stream, setup, wait_until};

fn key_of(input: &str) -> String {
    input.split(':').next().unwrap_or(input).to_string()
}

/// A `String`-in, `()`-state dynamic-mode Tasklet keyed by [`key_of`]. The
/// base form takes the `init_state` + `run` bodies; `dyn_tasklet!` adds
/// trivial ones plus a paired single-entry Relay. Extra trait items (TTL,
/// cap) are appended verbatim.
macro_rules! dyn_tasklet_base {
    ($tasklet:ident, $relay:ident, $name:tt, $factory:literal, { $($item:item)* }) => {
        #[derive(Default)]
        struct $tasklet;
        #[flezha_runner::tasklet(name = $name)]
        impl Tasklet for $tasklet {
            type Input = String;
            $($item)*
        }
        struct $relay;
        impl Relay for $relay {
            fn factory_id() -> &'static str { $factory }
            fn channels(c: &ChannelBuilder) {
                c.input::<String>("inp")
                    .filter(|s: &String| s != "drop")
                    .key_by(|s: &String| key_of(s))
                    .into::<$tasklet>();
            }
}
    };
}

macro_rules! dyn_tasklet {
    ($tasklet:ident, $relay:ident, $name:tt, $factory:literal $($extra:item)*) => {
        dyn_tasklet_base!($tasklet, $relay, $name, $factory, {
            fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
                Ok(())
            }
            $($extra)*
        });
    };
}

dyn_tasklet!(DynTasklet, DynRelay, "dyn_tasklet", "dyn_relay");
dyn_tasklet!(CappedTasklet, CappedRelay, "capped_tasklet", "capped_relay"
    fn max_cells() -> usize { 2 }
);
dyn_tasklet!(TtlTasklet, TtlRelay, "ttl_tasklet", "ttl_relay"
    fn cell_idle_ttl() -> Option<Duration> { Some(Duration::from_millis(100)) }
);

static SLEEP_RUN_STARTED: AtomicBool = AtomicBool::new(false);
static SLEEP_RUN_RELEASE: AtomicBool = AtomicBool::new(false);
static SLEEP_RUN_DONE: AtomicBool = AtomicBool::new(false);
dyn_tasklet_base!(SleepTasklet, SleepRelay, "sleep_tasklet", "sleep_relay", {
    fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        // Held until the test releases it: a run that lasts exactly as
        // long as the sweep it must survive.
        SLEEP_RUN_STARTED.store(true, Ordering::Release);
        common::wait_until(
            || SLEEP_RUN_RELEASE.load(Ordering::Acquire),
            Duration::from_secs(30),
        );
        SLEEP_RUN_DONE.store(true, Ordering::Release);
        Ok(())
    }
});

static INIT_KEYS: Mutex<Vec<String>> = Mutex::new(Vec::new());
dyn_tasklet_base!(
    InitTrackTasklet,
    InitTrackRelay,
    "init_track_tasklet",
    "init_track_relay",
    {
        fn init_state(key: Option<&str>) {
            if let Some(k) = key {
                INIT_KEYS.lock().unwrap().push(k.to_string());
            }
        }
        fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
            Ok(())
        }
    }
);

/// The single dynamic registry on a one-tasklet relay instance.
fn registry(instance: &Arc<RelayInstance>) -> Arc<DynamicCellRegistry> {
    Arc::clone(
        instance.dynamic_registries[0]
            .as_ref()
            .expect("tasklet 0 is dynamic"),
    )
}

fn created(reg: &DynamicCellRegistry) -> u64 {
    reg.counters.cells_created_total.load(Ordering::Relaxed)
}
fn evicted(reg: &DynamicCellRegistry) -> u64 {
    reg.counters.cells_evicted_total.load(Ordering::Relaxed)
}

#[test]
fn lazy_create_two_keys() {
    let (handle, instance) = setup::<DynRelay>("dyn-2");
    let stream = dummy_stream();
    dispatch_input(&handle, "dyn-2", "inp", b"a:1".to_vec(), &stream);
    dispatch_input(&handle, "dyn-2", "inp", b"b:2".to_vec(), &stream);

    let reg = registry(&instance);
    assert_eq!(reg.cells.len(), 2, "two distinct keys → two slots");
    assert_eq!(created(&reg), 2);
}

#[test]
fn soft_cap_drops_overflow_key() {
    let (handle, instance) = setup::<CappedRelay>("capped-1");
    let stream = dummy_stream();
    for inp in [b"a:1", b"b:2", b"c:3"] {
        dispatch_input(&handle, "capped-1", "inp", inp.to_vec(), &stream);
    }

    let reg = registry(&instance);
    assert_eq!(reg.cells.len(), 2, "cap = 2 → at most two slots");
    assert_eq!(
        reg.counters
            .inputs_dropped_capped_total
            .load(Ordering::Relaxed),
        1,
        "the third distinct key is dropped"
    );
}

#[test]
fn filtered_inputs_reach_no_cell() {
    let (handle, instance) = setup::<DynRelay>("dyn-nokey");
    let stream = dummy_stream();
    // 10 inputs; 4 "drop" sentinels → 6 slot creations.
    for inp in [
        "a:1", "drop", "b:2", "drop", "c:3", "drop", "d:4", "drop", "e:5", "f:6",
    ] {
        dispatch_input(
            &handle,
            "dyn-nokey",
            "inp",
            inp.as_bytes().to_vec(),
            &stream,
        );
    }

    let reg = registry(&instance);
    assert!(wait_until(|| reg.cells.len() == 6, Duration::from_secs(10)));
    assert_eq!(created(&reg), 6, "the four filtered inputs create nothing");
}

/// KB-063 / KB-086: the delete an eviction owes is a durable frame. With
/// nothing bound it must be HELD for the reconnect, and replayed as the exact
/// bytes the live path would have written: the controller reads the message
/// as the bare identity (`Flezha.State.Wire.decode_cell_id/1`), so any other
/// framing would be applied as garbage rather than dropped.
#[test]
fn an_eviction_with_no_stream_bound_holds_the_delete_for_the_reconnect() {
    let _guard = state_sink::test_guard();
    state_sink::set_secondary_stream(None);

    let (_handle, instance) = setup::<DynRelay>("dyn-delete-1");
    let reg = registry(&instance);
    let identity = reg
        .get_or_create("K")
        .expect("the key mints a Cell")
        .cell_identity
        .to_vec();

    // Backdate the activity stamp so the sweep decides on the TTL, not on the
    // clock, and evict with no output and no secondary: the reconnect window.
    reg.cells
        .get("K")
        .expect("live before the sweep")
        .last_activity_ms
        .store(-1_000, Ordering::Relaxed);
    reg.reap_sweep(Duration::ZERO);
    assert!(reg.cells.is_empty(), "the idle Cell is evicted");
    assert_eq!(evicted(&reg), 1);

    // The reconnect: the output an install wires must receive the held delete.
    let seen: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_c = Arc::clone(&seen);
    let sink: SendFrameFn = Arc::new(move |payload_id: &str, frame: &[u8]| {
        seen_c
            .lock()
            .unwrap()
            .push(payloader::build_without_metadata(payload_id, frame));
        Ok(())
    });
    install_global_sink(sink);

    let expected =
        payloader::build_without_metadata(state_sink::DELETE_CELL_STATE_PAYLOAD_ID, &identity);
    assert!(
        seen.lock().unwrap().contains(&expected),
        "an eviction during a reconnect window must hold its delete, framed \
         exactly as the live path frames it"
    );

    // The output is process-global: hand the rest of this binary a no-op rather
    // than a recorder that keeps every frame they ship.
    install_global_sink(Arc::new(|_: &str, _: &[u8]| Ok(())));
}

#[test]
fn idle_cell_is_reaped() {
    // Reaper sweeps every 25ms; the 100ms-TTL Cell is evicted shortly
    // after it goes idle. Generous deadline absorbs full-suite scheduling
    // starvation (the reaper retries every sweep).
    std::env::set_var("FLEZHA_REAP_INTERVAL_MS", "25");
    let (handle, instance) = setup::<TtlRelay>("ttl-1");
    let stream = dummy_stream();
    dispatch_input(&handle, "ttl-1", "inp", b"K:1".to_vec(), &stream);

    // Key off the eviction counter, not the map: the reaper removes the
    // entry a hair before bumping the counter, so a cross-thread watcher of
    // `cells` could otherwise win that window. Generous deadline
    // absorbs full-suite scheduling starvation (the reaper retries each sweep).
    let reg = registry(&instance);
    assert!(wait_until(|| evicted(&reg) == 1, Duration::from_secs(15)));
    assert!(reg.cells.is_empty());
}

#[test]
fn reaper_skips_cell_with_active_run() {
    SLEEP_RUN_STARTED.store(false, Ordering::Release);
    SLEEP_RUN_RELEASE.store(false, Ordering::Release);
    SLEEP_RUN_DONE.store(false, Ordering::Release);
    let (handle, instance) = setup::<SleepRelay>("sleep-1");
    let stream = dummy_stream();
    dispatch_input(&handle, "sleep-1", "inp", b"K:1".to_vec(), &stream);
    let reg = registry(&instance);

    assert!(wait_until(
        || SLEEP_RUN_STARTED.load(Ordering::Acquire),
        Duration::from_secs(10)
    ));
    // The run holds the drain gate; a sweep (even at 1ms TTL) must not evict.
    reg.reap_sweep(Duration::from_millis(1));
    assert_eq!(reg.cells.len(), 1, "evicted while Tasklet::run was active");
    assert_eq!(evicted(&reg), 0);

    // Once the run releases the gate, the next sweep evicts.
    SLEEP_RUN_RELEASE.store(true, Ordering::Release);
    assert!(wait_until(
        || SLEEP_RUN_DONE.load(Ordering::Acquire),
        Duration::from_secs(10)
    ));
    assert!(wait_until(
        || {
            reg.reap_sweep(Duration::from_millis(1));
            reg.cells.is_empty()
        },
        Duration::from_secs(10)
    ));
    assert_eq!(evicted(&reg), 1);
}

#[test]
fn recreate_after_evict_cold_starts_fresh() {
    INIT_KEYS.lock().unwrap().clear();
    let (handle, instance) = setup::<InitTrackRelay>("recreate-1");
    let stream = dummy_stream();
    let reg = registry(&instance);

    dispatch_input(&handle, "recreate-1", "inp", b"K:1".to_vec(), &stream);
    assert!(wait_until(|| reg.cells.len() == 1, Duration::from_secs(10)));

    assert!(wait_until(
        || {
            reg.reap_sweep(Duration::from_millis(1));
            reg.cells.is_empty()
        },
        Duration::from_secs(10)
    ));

    // Key returns → a brand-new slot + a SECOND init_state(Some("K")): a cold
    // recreate, not a restore.
    dispatch_input(&handle, "recreate-1", "inp", b"K:2".to_vec(), &stream);
    assert!(wait_until(|| reg.cells.len() == 1, Duration::from_secs(10)));

    assert_eq!(
        *INIT_KEYS.lock().unwrap(),
        vec!["K".to_string(), "K".to_string()]
    );
    assert_eq!(created(&reg), 2);
    assert_eq!(evicted(&reg), 1);
}

#[test]
fn concurrent_producers_share_one_slot() {
    let (handle, instance) = setup::<DynRelay>("dyn-concurrent");
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
                dispatch_input(
                    &handle,
                    "dyn-concurrent",
                    "inp",
                    format!("K:{i}").into_bytes(),
                    &stream,
                );
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }

    // DashMap serialized the inserts: one slot, created once, all 16 landed.
    assert_eq!(reg.cells.len(), 1);
    assert_eq!(created(&reg), 1);
    let slot = reg.cells.get("K").expect("slot for K");
    assert!(wait_until(
        || slot.counters().pushes_total.load(Ordering::Relaxed) == N as u64,
        Duration::from_secs(10)
    ));
}
