//! dynamic entity mode runtime tests: lazy create, soft cap,
//! filtered-out inputs (§2); idle-TTL eviction and the CAS-vs-reaper race
//! (§3); concurrent-producer + cold-recreate (§4).
//!
//! The dynamic key is declared on the WIRING: the substring before the first
//! `:`. There is no keyless-drop path — the literal `"drop"` is removed by a
//! `filter` declared before the keying, which is how an input is discarded.

use std::sync::{Arc, Barrier, Mutex};
use std::thread;

use flezha_runner::entity::dynamic::DynamicEntityRegistry;

mod common;
use common::prelude::*;
use common::{dispatch_input, dummy_stream, setup, wait_until};

fn key_of(input: &str) -> String {
    input.split(':').next().unwrap_or(input).to_string()
}

/// A `String`-in, `()`-state dynamic-mode Procedure keyed by [`key_of`]. The
/// base form takes the `init_state` + `run` bodies; `dyn_procedure!` adds
/// trivial ones plus a paired single-entry StreamOperative. Extra trait items (TTL,
/// cap) are appended verbatim.
macro_rules! dyn_procedure_base {
    ($procedure:ident, $operative:ident, $name:literal, $factory:literal, { $($item:item)* }) => {
        #[derive(Default)]
        struct $procedure;
        impl Procedure for $procedure {
            type Input = String;
            type Output = ();
            type State = ();
            type Change = NoChange;
            type Clock = WallClock;
            type Config = ();
            fn name() -> &'static str { $name }
            $($item)*
        }
        struct $operative;
        impl StreamOperative for $operative {
            fn factory_id() -> &'static str { $factory }
            fn channels(c: &ChannelBuilder) {
                c.input::<String>("inp")
                    .filter(|s: &String| s != "drop")
                    .key_by(|s: &String| key_of(s))
                    .into::<$procedure>();
            }
        }
    };
}

macro_rules! dyn_procedure {
    ($procedure:ident, $operative:ident, $name:literal, $factory:literal $($extra:item)*) => {
        dyn_procedure_base!($procedure, $operative, $name, $factory, {
            fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
            fn run(&mut self, _: String, _: StateHandle<'_, Self>, _: &mut ProcedureCtx<'_>)
                -> Result<(), ProcedureError> { Ok(()) }
            $($extra)*
        });
    };
}

dyn_procedure!(DynProcedure, DynOperative, "dyn_procedure", "dyn_operative");
dyn_procedure!(CappedProcedure, CappedOperative, "capped_procedure", "capped_operative"
    fn max_entities() -> usize { 2 }
    const MAX_ENTITIES_OVERRIDDEN: bool = true;
);
dyn_procedure!(TtlProcedure, TtlOperative, "ttl_procedure", "ttl_operative"
    fn entity_idle_ttl() -> Option<Duration> { Some(Duration::from_millis(100)) }
);

static SLEEP_RUN_STARTED: AtomicBool = AtomicBool::new(false);
static SLEEP_RUN_RELEASE: AtomicBool = AtomicBool::new(false);
static SLEEP_RUN_DONE: AtomicBool = AtomicBool::new(false);
dyn_procedure_base!(
    SleepProcedure,
    SleepOperative,
    "sleep_procedure",
    "sleep_operative",
    {
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            _: String,
            _: StateHandle<'_, Self>,
            _: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
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
    }
);

static INIT_KEYS: Mutex<Vec<String>> = Mutex::new(Vec::new());
dyn_procedure_base!(
    InitTrackProcedure,
    InitTrackOperative,
    "init_track_procedure",
    "init_track_operative",
    {
        fn init_state(key: Option<&str>, _: Option<RestoreInitPayload<'_>>) {
            if let Some(k) = key {
                INIT_KEYS.lock().unwrap().push(k.to_string());
            }
        }
        fn run(
            &mut self,
            _: String,
            _: StateHandle<'_, Self>,
            _: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            Ok(())
        }
    }
);

/// The single dynamic registry on a one-procedure operative instance.
fn registry(instance: &Arc<StreamInstance>) -> Arc<DynamicEntityRegistry> {
    Arc::clone(
        instance.dynamic_registries[0]
            .as_ref()
            .expect("procedure 0 is dynamic"),
    )
}

fn created(reg: &DynamicEntityRegistry) -> u64 {
    reg.counters.entities_created_total.load(Ordering::Relaxed)
}
fn evicted(reg: &DynamicEntityRegistry) -> u64 {
    reg.counters.entities_evicted_total.load(Ordering::Relaxed)
}

#[test]
fn lazy_create_two_keys() {
    let (handle, instance) = setup::<DynOperative>("dyn-2");
    let stream = dummy_stream();
    dispatch_input(&handle, "dyn-2", "inp", b"a:1".to_vec(), &stream);
    dispatch_input(&handle, "dyn-2", "inp", b"b:2".to_vec(), &stream);

    let reg = registry(&instance);
    assert_eq!(reg.entities.len(), 2, "two distinct keys → two slots");
    assert_eq!(created(&reg), 2);
}

#[test]
fn soft_cap_drops_overflow_key() {
    let (handle, instance) = setup::<CappedOperative>("capped-1");
    let stream = dummy_stream();
    for inp in [b"a:1", b"b:2", b"c:3"] {
        dispatch_input(&handle, "capped-1", "inp", inp.to_vec(), &stream);
    }

    let reg = registry(&instance);
    assert_eq!(reg.entities.len(), 2, "cap = 2 → at most two slots");
    assert_eq!(
        reg.counters
            .inputs_dropped_capped_total
            .load(Ordering::Relaxed),
        1,
        "the third distinct key is dropped"
    );
}

#[test]
fn filtered_inputs_reach_no_entity() {
    let (handle, instance) = setup::<DynOperative>("dyn-nokey");
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
    assert!(wait_until(
        || reg.entities.len() == 6,
        Duration::from_secs(10)
    ));
    assert_eq!(created(&reg), 6, "the four filtered inputs create nothing");
}

#[test]
fn idle_entity_is_reaped() {
    // Reaper sweeps every 25ms; the 100ms-TTL Entity is evicted shortly
    // after it goes idle. Generous deadline absorbs full-suite scheduling
    // starvation (the reaper retries every sweep).
    std::env::set_var("FLEZHA_REAP_INTERVAL_MS", "25");
    let (handle, instance) = setup::<TtlOperative>("ttl-1");
    let stream = dummy_stream();
    dispatch_input(&handle, "ttl-1", "inp", b"K:1".to_vec(), &stream);

    // Key off the eviction counter, not the map: the reaper removes the
    // entry a hair before bumping the counter, so a cross-thread watcher of
    // `entities` could otherwise win that window. Generous deadline
    // absorbs full-suite scheduling starvation (the reaper retries each sweep).
    let reg = registry(&instance);
    assert!(wait_until(|| evicted(&reg) == 1, Duration::from_secs(15)));
    assert!(reg.entities.is_empty());
}

#[test]
fn reaper_skips_entity_with_active_run() {
    SLEEP_RUN_STARTED.store(false, Ordering::Release);
    SLEEP_RUN_RELEASE.store(false, Ordering::Release);
    SLEEP_RUN_DONE.store(false, Ordering::Release);
    let (handle, instance) = setup::<SleepOperative>("sleep-1");
    let stream = dummy_stream();
    dispatch_input(&handle, "sleep-1", "inp", b"K:1".to_vec(), &stream);
    let reg = registry(&instance);

    assert!(wait_until(
        || SLEEP_RUN_STARTED.load(Ordering::Acquire),
        Duration::from_secs(10)
    ));
    // The run holds the drain gate; a sweep (even at 1ms TTL) must not evict.
    reg.reap_sweep(Duration::from_millis(1));
    assert_eq!(
        reg.entities.len(),
        1,
        "evicted while Procedure::run was active"
    );
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
            reg.entities.is_empty()
        },
        Duration::from_secs(10)
    ));
    assert_eq!(evicted(&reg), 1);
}

#[test]
fn recreate_after_evict_cold_starts_fresh() {
    INIT_KEYS.lock().unwrap().clear();
    let (handle, instance) = setup::<InitTrackOperative>("recreate-1");
    let stream = dummy_stream();
    let reg = registry(&instance);

    dispatch_input(&handle, "recreate-1", "inp", b"K:1".to_vec(), &stream);
    assert!(wait_until(
        || reg.entities.len() == 1,
        Duration::from_secs(10)
    ));

    assert!(wait_until(
        || {
            reg.reap_sweep(Duration::from_millis(1));
            reg.entities.is_empty()
        },
        Duration::from_secs(10)
    ));

    // Key returns → a brand-new slot + a SECOND init_state(Some("K")): a cold
    // recreate, not a restore.
    dispatch_input(&handle, "recreate-1", "inp", b"K:2".to_vec(), &stream);
    assert!(wait_until(
        || reg.entities.len() == 1,
        Duration::from_secs(10)
    ));

    assert_eq!(
        *INIT_KEYS.lock().unwrap(),
        vec!["K".to_string(), "K".to_string()]
    );
    assert_eq!(created(&reg), 2);
    assert_eq!(evicted(&reg), 1);
}

#[test]
fn concurrent_producers_share_one_slot() {
    let (handle, instance) = setup::<DynOperative>("dyn-concurrent");
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
    assert_eq!(reg.entities.len(), 1);
    assert_eq!(created(&reg), 1);
    let slot = reg.entities.get("K").expect("slot for K");
    assert!(wait_until(
        || slot.counters().pushes_total.load(Ordering::Relaxed) == N as u64,
        Duration::from_secs(10)
    ));
}
