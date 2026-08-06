//! Dynamic-mode entity registry (dynamic entity mode).
//!
//! A dynamic-mode Procedure owns zero Entities at startup; the dispatch path
//! derives a key via the consumer edge's `key_by` and [`DynamicEntityRegistry::get_or_create`]
//! lazily allocates an `EntitySlot` on first sight. With a TTL set, a
//! per-Procedure reaper thread evicts idle Entities (destructively — see
//! [`DynamicEntityRegistry::reap_sweep`]); a later input cold-starts a
//! fresh Entity.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use flz_transport::service::payloader;

use crate::channel::builder::BuildEntityArgs;
use crate::channel::ProcedureId;
use crate::entity::flusher::EntityFlusher;
use crate::entity::{state_sink, EntitySlot};
use crate::operative::dispatcher::drain_entity;
use crate::operative::instance::UnboundedInstance;
use crate::operative::topology::OperativeTopology;
use crate::runner::work_stealing_pool::WorkStealingPool;

const ENV_REAP_INTERVAL_MS: &str = "FLEZHA_REAP_INTERVAL_MS";
const MAX_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Monotonic milliseconds since process start. Stamped on every push into a
/// dynamic Entity's mailbox; read by the reaper. One global anchor keeps
/// `EntitySlot::last_activity_at` a plain `AtomicI64`.
pub fn now_millis() -> i64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as i64
}

/// Per-Procedure dynamic-entity counters. Monotonic, `Relaxed`.
#[derive(Debug, Default)]
pub struct DynamicEntityCounters {
    pub entities_created_total: AtomicU64,
    pub entities_evicted_total: AtomicU64,
    pub inputs_dropped_capped_total: AtomicU64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum GetOrCreateError {
    /// The per-Procedure `max_entities` cap is reached; the new-key input is
    /// dropped (`inputs_dropped_capped_total` bumped).
    Capped,
}

/// Lazily-populated per-Procedure entity map plus the build context needed to
/// allocate fresh slots. Owned by the `UnboundedInstance`, one per dynamic Procedure.
pub struct DynamicEntityRegistry {
    pub entities: DashMap<String, Arc<EntitySlot>>,
    pub counters: DynamicEntityCounters,
    procedure_id: ProcedureId,
    max_entities: usize,
    topology: Arc<OperativeTopology>,
    instance: Weak<UnboundedInstance>,
    instance_id: Arc<str>,
    pool: Arc<WorkStealingPool>,
    persistence_enabled: bool,
}

impl DynamicEntityRegistry {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        procedure_id: ProcedureId,
        max_entities: usize,
        topology: Arc<OperativeTopology>,
        instance: Weak<UnboundedInstance>,
        instance_id: Arc<str>,
        pool: Arc<WorkStealingPool>,
        persistence_enabled: bool,
    ) -> Self {
        Self {
            entities: DashMap::new(),
            counters: DynamicEntityCounters::default(),
            procedure_id,
            max_entities,
            topology,
            instance,
            instance_id,
            pool,
            persistence_enabled,
        }
    }

    /// Return the Entity for `key`, lazily creating it. The cap check is
    /// best-effort (concurrent inserts may slightly overshoot — the cap
    /// bounds memory, not admission).
    pub fn get_or_create(&self, key: &str) -> Result<Arc<EntitySlot>, GetOrCreateError> {
        if let Some(slot) = self.entities.get(key) {
            return Ok(Arc::clone(slot.value()));
        }
        if self.entities.len() >= self.max_entities {
            self.counters
                .inputs_dropped_capped_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(GetOrCreateError::Capped);
        }
        let entry = self.entities.entry(key.to_owned()).or_insert_with(|| {
            self.counters
                .entities_created_total
                .fetch_add(1, Ordering::Relaxed);
            self.build_slot(key)
        });
        Ok(Arc::clone(entry.value()))
    }

    /// One reaper sweep: evict every Entity idle longer than `idle_ttl`.
    /// Eviction acquires the slot's drain gate (never interrupts a running
    /// `Procedure::run`), confirms the mailbox is empty, then destructively drops
    /// the slot — deregistering its flusher entry and (when persistent)
    /// shipping `delete_entity_state` so the on-disk WAL/snapshot is removed.
    pub fn reap_sweep(&self, idle_ttl: Duration) {
        let now = now_millis();
        let ttl_ms = idle_ttl.as_millis() as i64;

        // Collect idle candidates, taking each one's drain gate. Removal is
        // deferred until the iterator drops (mutating mid-iteration would
        // deadlock on the DashMap procedure lock).
        let mut candidates: Vec<(String, Arc<EntitySlot>)> = Vec::new();
        for entry in self.entities.iter() {
            let slot = entry.value();
            if now - slot.last_activity_at.load(Ordering::Relaxed) <= ttl_ms {
                continue;
            }
            if !slot.mailbox.try_mark_inflight() {
                continue; // a drain is scheduled/running; retry next sweep
            }
            if !slot.mailbox.is_empty() {
                slot.mailbox.release_inflight(); // queued work; not idle
                continue;
            }
            candidates.push((entry.key().clone(), Arc::clone(slot)));
        }

        for (key, slot) in candidates {
            self.entities.remove(&key);
            // Close the lookup-then-push race: a producer holding the slot Arc
            // may have pushed after our empty-check. Re-insert and drain it.
            if !slot.mailbox.is_empty() {
                self.entities.insert(key, Arc::clone(&slot));
                slot.mailbox.release_inflight();
                if slot.mailbox.try_mark_inflight() {
                    let cloned = Arc::clone(&slot);
                    slot.pool.submit(Box::new(move || drain_entity(&cloned)));
                }
                continue;
            }
            if let Some(id) = slot.flusher_entry_id {
                EntityFlusher::instance().deregister(id);
            }
            if slot.persistence_enabled {
                send_delete_entity_state(&slot.entity_identity);
            }
            self.counters
                .entities_evicted_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Allocate a fresh slot for `key`, wired exactly as the static-mode slots
    /// `UnboundedInstance::new` builds.
    fn build_slot(&self, key: &str) -> Arc<EntitySlot> {
        let procedure_desc = self
            .topology
            .procedure(self.procedure_id)
            .expect("dynamic registry: procedure id missing in topology");
        (procedure_desc.build_entity)(BuildEntityArgs {
            topology: Arc::clone(&self.topology),
            instance: Weak::clone(&self.instance),
            instance_id: Arc::clone(&self.instance_id),
            procedure_id: self.procedure_id,
            entity_key: Some(key),
            entity_index: 0,
            persistence_enabled: self.persistence_enabled,
            pool: Arc::clone(&self.pool),
        })
    }
}

/// Spawn one reaper thread for a dynamic Procedure. It sweeps every
/// `reap_interval(idle_ttl)` until `stop` is set. Only called when a TTL is
/// declared (default `None` spawns no reaper).
pub fn spawn_reaper(
    idle_ttl: Duration,
    registry: Arc<DynamicEntityRegistry>,
    stop: Arc<AtomicBool>,
) -> JoinHandle<()> {
    let interval = reap_interval(idle_ttl);
    thread::Builder::new()
        .name("flezha-reaper".to_string())
        .spawn(move || {
            while !stop.load(Ordering::Acquire) {
                sleep_responsive(interval, &stop);
                if !stop.load(Ordering::Acquire) {
                    registry.reap_sweep(idle_ttl);
                }
            }
        })
        .expect("spawning flezha-reaper thread should not fail")
}

/// `FLEZHA_REAP_INTERVAL_MS` if set, else `min(idle_ttl / 4, 30s)`, floored at 1ms.
fn reap_interval(idle_ttl: Duration) -> Duration {
    std::env::var(ENV_REAP_INTERVAL_MS)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(|ms| Duration::from_millis(ms.max(1)))
        .unwrap_or(
            (idle_ttl / 4)
                .min(MAX_SWEEP_INTERVAL)
                .max(Duration::from_millis(1)),
        )
}

/// Sleep `total`, waking every ≤200ms to honour `stop` promptly.
fn sleep_responsive(total: Duration, stop: &AtomicBool) {
    let chunk = total.min(Duration::from_millis(200));
    let mut slept = Duration::ZERO;
    while slept < total && !stop.load(Ordering::Acquire) {
        thread::sleep(chunk);
        slept += chunk;
    }
}

/// Fire-and-forget `delete_entity_state` for an evicted Entity (body is
/// the structured `entity_id`).
fn send_delete_entity_state(entity_id: &[u8]) {
    let frame = payloader::build_from_binary(
        state_sink::DELETE_ENTITY_STATE_PAYLOAD_ID,
        entity_id,
        HashMap::new(),
    );
    let _ = state_sink::send_oneshot(&frame);
}
