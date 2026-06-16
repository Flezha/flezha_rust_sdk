//! Dynamic-mode partition registry (`relay-dynamic-partitioning`).
//!
//! A dynamic-mode Shard owns zero Partitions at startup; the dispatch path
//! derives a key via `Shard::shard_key` and [`DynamicPartitionRegistry::get_or_create`]
//! lazily allocates a `PartitionSlot` on first sight. With a TTL set, a
//! per-Shard reaper thread evicts idle Partitions (destructively — see
//! [`DynamicPartitionRegistry::reap_sweep`]); a later input cold-starts a
//! fresh Partition.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use flz_transport::service::payloader;

use crate::channel::builder::BuildPartitionArgs;
use crate::channel::ShardId;
use crate::executor::work_stealing_pool::WorkStealingPool;
use crate::partition::flusher::PartitionFlusher;
use crate::partition::{partition_state_sink, PartitionSlot};
use crate::relay::dispatcher::drain_partition;
use crate::relay::instance::RelayInstance;
use crate::relay::topology::RelayTopology;

const ENV_REAP_INTERVAL_MS: &str = "FLEZHA_REAP_INTERVAL_MS";
const MAX_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Monotonic milliseconds since process start. Stamped on every push into a
/// dynamic Partition's mailbox; read by the reaper. One global anchor keeps
/// `PartitionSlot::last_activity_at` a plain `AtomicI64`.
pub fn now_millis() -> i64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as i64
}

/// Per-Shard dynamic-partition counters. Monotonic, `Relaxed`.
#[derive(Debug, Default)]
pub struct DynamicPartitionCounters {
    pub partitions_created_total: AtomicU64,
    pub partitions_evicted_total: AtomicU64,
    pub inputs_dropped_no_key_total: AtomicU64,
    pub inputs_dropped_capped_total: AtomicU64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum GetOrCreateError {
    /// The per-Shard `max_partitions` cap is reached; the new-key input is
    /// dropped (`inputs_dropped_capped_total` bumped).
    Capped,
}

/// Lazily-populated per-Shard partition map plus the build context needed to
/// allocate fresh slots. Owned by the `RelayInstance`, one per dynamic Shard.
pub struct DynamicPartitionRegistry {
    pub partitions: DashMap<String, Arc<PartitionSlot>>,
    pub counters: DynamicPartitionCounters,
    shard_id: ShardId,
    max_partitions: usize,
    topology: Arc<RelayTopology>,
    instance: Weak<RelayInstance>,
    instance_id: Arc<str>,
    pool: Arc<WorkStealingPool>,
    persistence_enabled: bool,
}

impl DynamicPartitionRegistry {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shard_id: ShardId,
        max_partitions: usize,
        topology: Arc<RelayTopology>,
        instance: Weak<RelayInstance>,
        instance_id: Arc<str>,
        pool: Arc<WorkStealingPool>,
        persistence_enabled: bool,
    ) -> Self {
        Self {
            partitions: DashMap::new(),
            counters: DynamicPartitionCounters::default(),
            shard_id,
            max_partitions,
            topology,
            instance,
            instance_id,
            pool,
            persistence_enabled,
        }
    }

    /// Return the Partition for `key`, lazily creating it. The cap check is
    /// best-effort (concurrent inserts may slightly overshoot — the cap
    /// bounds memory, not admission).
    pub fn get_or_create(&self, key: &str) -> Result<Arc<PartitionSlot>, GetOrCreateError> {
        if let Some(slot) = self.partitions.get(key) {
            return Ok(Arc::clone(slot.value()));
        }
        if self.partitions.len() >= self.max_partitions {
            self.counters.inputs_dropped_capped_total.fetch_add(1, Ordering::Relaxed);
            return Err(GetOrCreateError::Capped);
        }
        let entry = self.partitions.entry(key.to_owned()).or_insert_with(|| {
            self.counters.partitions_created_total.fetch_add(1, Ordering::Relaxed);
            self.build_slot(key)
        });
        Ok(Arc::clone(entry.value()))
    }

    /// One reaper sweep: evict every Partition idle longer than `idle_ttl`.
    /// Eviction acquires the slot's drain gate (never interrupts a running
    /// `Shard::run`), confirms the mailbox is empty, then destructively drops
    /// the slot — deregistering its flusher entry and (when persistent)
    /// shipping `delete_partition_state` so the on-disk WAL/snapshot is removed.
    pub fn reap_sweep(&self, idle_ttl: Duration) {
        let now = now_millis();
        let ttl_ms = idle_ttl.as_millis() as i64;

        // Collect idle candidates, taking each one's drain gate. Removal is
        // deferred until the iterator drops (mutating mid-iteration would
        // deadlock on the DashMap shard lock).
        let mut candidates: Vec<(String, Arc<PartitionSlot>)> = Vec::new();
        for entry in self.partitions.iter() {
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
            self.partitions.remove(&key);
            // Close the lookup-then-push race: a producer holding the slot Arc
            // may have pushed after our empty-check. Re-insert and drain it.
            if !slot.mailbox.is_empty() {
                self.partitions.insert(key, Arc::clone(&slot));
                slot.mailbox.release_inflight();
                if slot.mailbox.try_mark_inflight() {
                    let cloned = Arc::clone(&slot);
                    slot.pool.submit(Box::new(move || drain_partition(&cloned)));
                }
                continue;
            }
            if let Some(id) = slot.flusher_entry_id {
                PartitionFlusher::instance().deregister(id);
            }
            if slot.persistence_enabled {
                send_delete_partition_state(&slot.partition_identity);
            }
            self.counters.partitions_evicted_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Allocate a fresh slot for `key`, wired exactly as the static-mode slots
    /// `RelayInstance::new` builds.
    fn build_slot(&self, key: &str) -> Arc<PartitionSlot> {
        let shard_desc = self
            .topology
            .shard(self.shard_id)
            .expect("dynamic registry: shard id missing in topology");
        (shard_desc.build_partition)(BuildPartitionArgs {
            topology: Arc::clone(&self.topology),
            instance: Weak::clone(&self.instance),
            instance_id: Arc::clone(&self.instance_id),
            shard_id: self.shard_id,
            shard_key: Some(key),
            partition_index: 0,
            persistence_enabled: self.persistence_enabled,
            pool: Arc::clone(&self.pool),
        })
    }
}

/// Spawn one reaper thread for a dynamic Shard. It sweeps every
/// `reap_interval(idle_ttl)` until `stop` is set. Only called when a TTL is
/// declared (default `None` spawns no reaper).
pub fn spawn_reaper(
    idle_ttl: Duration,
    registry: Arc<DynamicPartitionRegistry>,
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
        .unwrap_or((idle_ttl / 4).min(MAX_SWEEP_INTERVAL).max(Duration::from_millis(1)))
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

/// Fire-and-forget `delete_partition_state` for an evicted Partition (body is
/// the structured `partition_id`).
fn send_delete_partition_state(partition_id: &[u8]) {
    let frame = payloader::build_from_binary(
        partition_state_sink::DELETE_PARTITION_STATE_PAYLOAD_ID,
        partition_id,
        HashMap::new(),
    );
    let _ = partition_state_sink::send_oneshot(&frame);
}
