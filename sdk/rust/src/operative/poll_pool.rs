//! The typed layer's poll workers: this SDK's half of the poll/complete seam.
//!
//! The core decides what runs and caps how much may be underway; a host's only
//! job is to bring the threads (`docs/sdk/poll_core.md`). Rust has no cheap
//! blocking, so this host answers with a pool that grows: a base set that
//! never retires, plus one supervisor parked in the core's starvation wait —
//! work queued with no thread inside `poll` at all. Each observation adds one
//! thread, up to a ceiling, and the threads above the base retire after an
//! idle streak, so a workload that never starves observes exactly the base pool.
//!
//! That supervisor is also the pool's only self-heal: a worker that leaves for
//! any reason releases its slot through a guard that runs on an unwind, and the
//! supervisor restores the pool to `base` whenever it finds it short. A
//! shrunken-but-sufficient pool starves nobody, so waiting for a starvation
//! signal would leave the runner permanently below the operator's size.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::runner::poll_core::{global, PollCore, Polled, Starvation};

/// How many workers never retire. Defaults to the host's parallelism.
const ENV_WORKER_POOL_SIZE: &str = "FLEZHA_WORKER_POOL_SIZE";
/// The ceiling the starvation supervisor may grow the pool to.
const ENV_WORKER_POOL_MAX: &str = "FLEZHA_WORKER_POOL_MAX";
/// One poll turn. Also the granularity of a grown worker's idle streak and of
/// how fast a worker notices shutdown.
const POLL_TIMEOUT: Duration = Duration::from_millis(200);
/// How long a grown worker polls without work before retiring.
const IDLE_EXIT_AFTER: Duration = Duration::from_secs(30);
/// The supervisor's park length. Only bounds how often it re-arms — the core
/// decides when the wait returns.
const SUPERVISOR_WAIT: Duration = Duration::from_secs(1);

/// One poll-worker pool over one core.
struct PollPool {
    core: &'static PollCore,
    base: usize,
    max: usize,
    live: AtomicUsize,
    next_id: AtomicUsize,
    /// How long a grown worker polls without work before retiring. A field
    /// rather than a constant so the pool's own tests need not run for the
    /// production streak.
    idle_exit: Duration,
}

/// How many workers this process keeps at rest. Also what a test reads to size
/// a workload against the pool.
pub(crate) fn base_worker_count() -> usize {
    flz_core::util::env_parse_positive(
        ENV_WORKER_POOL_SIZE,
        std::thread::available_parallelism().map_or(2, |n| n.get()),
    )
}

impl PollPool {
    fn from_env(core: &'static PollCore) -> Arc<Self> {
        let base = base_worker_count();
        Arc::new(Self {
            core,
            base,
            max: flz_core::util::env_parse_positive(ENV_WORKER_POOL_MAX, base * 10).max(base),
            live: AtomicUsize::new(0),
            next_id: AtomicUsize::new(0),
            idle_exit: IDLE_EXIT_AFTER,
        })
    }

    /// Spawn the base workers, which never retire, plus the supervisor.
    fn start(self: &Arc<Self>) {
        self.live.store(self.base, Ordering::Release);
        for _ in 0..self.base {
            self.spawn_worker(false).expect("spawn poll worker");
        }
        let pool = Arc::clone(self);
        std::thread::Builder::new()
            .name("flz-poll-supervisor".to_string())
            .spawn(move || pool.supervise())
            .expect("spawn poll supervisor");
    }

    /// The pool's one control loop, with two conditions.
    ///
    /// Every starvation observation means work is queued with nobody inside
    /// `poll` to take it, so the answer is one more thread. Growth is
    /// self-limiting without a settle here: the spawned worker's own `poll`
    /// entry restarts the core's dwell, so the next thread is at least a dwell
    /// away and only if the runner is still stuck.
    ///
    /// The other condition is a pool that has gone SHORT. A worker that
    /// unwound out of its loop released its slot, and nothing else would ever
    /// bring it back: a shrunken-but-still-sufficient pool starves nobody, so
    /// waiting for a starvation signal would leave the runner permanently
    /// below the size the operator asked for. Every wake checks, so a
    /// replacement is at most one park behind the death.
    fn supervise(self: Arc<Self>) {
        loop {
            let starvation = self.core.wait_starvation(SUPERVISOR_WAIT);
            if matches!(starvation, Starvation::ShuttingDown) {
                return;
            }
            self.top_up();
            if matches!(starvation, Starvation::Observed) {
                self.grow();
            }
        }
    }

    /// Bring the pool back to `base`, one worker per missing slot. Each
    /// replacement is logged: a pool respawning into a repeating crash is a hot
    /// loop, and the log is what makes it visible.
    fn top_up(self: &Arc<Self>) {
        while self.spawn_into_slot(self.base, false) {
            tracing::warn!(
                target: "flezha::dispatch",
                base = self.base,
                "replaced a poll worker that left the pool"
            );
        }
    }

    /// Add one worker if the ceiling allows.
    fn grow(self: &Arc<Self>) {
        self.spawn_into_slot(self.max, true);
    }

    /// Reserve a slot below `limit` and spawn a worker into it, releasing the
    /// reservation again if the spawn fails. `false` means no worker joined —
    /// the limit left no room, or the thread could not be created — which is
    /// also what ends `top_up`'s loop.
    ///
    /// The slot is reserved BEFORE the spawn because a retiring worker may be
    /// decrementing concurrently and the ceiling has to hold exactly.
    fn spawn_into_slot(self: &Arc<Self>, limit: usize, above_base: bool) -> bool {
        if self
            .live
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |live| {
                (live < limit).then_some(live + 1)
            })
            .is_err()
        {
            return false;
        }
        if let Err(e) = self.spawn_worker(above_base) {
            self.live.fetch_sub(1, Ordering::AcqRel);
            tracing::warn!(
                target: "flezha::dispatch",
                error = %e,
                "could not spawn a poll worker"
            );
            return false;
        }
        true
    }

    fn spawn_worker(self: &Arc<Self>, above_base: bool) -> std::io::Result<()> {
        let pool = Arc::clone(self);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        std::thread::Builder::new()
            .name(format!("flz-poll-{id}"))
            .spawn(move || pool.work(above_base))
            .map(|_| ())
    }

    fn work(self: Arc<Self>, above_base: bool) {
        let _slot = LiveWorker(Arc::clone(&self));
        let mut idle = Duration::ZERO;
        loop {
            match self.core.poll(POLL_TIMEOUT) {
                Polled::Activation(act) => {
                    idle = Duration::ZERO;
                    super::dispatcher::run_activation(self.core, act);
                }
                Polled::Timeout => {
                    idle += POLL_TIMEOUT;
                    if above_base && idle >= self.idle_exit {
                        break;
                    }
                }
                Polled::ShuttingDown => break,
            }
        }
    }
}

/// A worker's claim on one pool slot, released on ANY exit — a clean
/// retirement, or an unwind straight past the loop. The live count is the
/// ceiling's denominator and the supervisor's top-up trigger, so a departed
/// worker still counted would both shrink the pool's headroom and hide the
/// death: an ordinary trailing `fetch_sub` is skipped by exactly the exit that
/// most needs it.
struct LiveWorker(Arc<PollPool>);

impl Drop for LiveWorker {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::AcqRel);
        if std::thread::panicking() {
            tracing::error!(
                target: "flezha::dispatch",
                "a poll worker panicked out of its loop; the supervisor will replace it"
            );
        }
    }
}

/// Start the process-wide pool (once): the host half of the poll/complete
/// seam. Called before the first input can be scheduled.
pub(crate) fn ensure_poll_workers() {
    static STARTED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    STARTED.get_or_init(|| {
        PollPool::from_env(global()).start();
    });
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    use super::*;
    use crate::channel::ChannelBuilder;
    use crate::entity::restore::RestoreInitPayload;
    use crate::operative::StreamOperative;
    use crate::procedure::state_handle::StateHandle;
    use crate::procedure::{Procedure, ProcedureCtx, ProcedureError};
    use crate::runner::poll_core_tests::fixtures::{build_slot_of, push_input, wait_until};

    /// Blocks on a process-wide gate for input `0`, returns immediately for
    /// anything else: the two halves of "one entity stuck in user code must
    /// not stop another entity's work".
    #[derive(Default)]
    struct GatedProc;

    static GATE_HELD: AtomicBool = AtomicBool::new(false);
    static SERVED: AtomicUsize = AtomicUsize::new(0);
    /// The gate and the counter are process-global, so the cases take turns.
    static POOL_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    impl Procedure for GatedProc {
        type Input = usize;
        type Output = ();
        type State = ();
        type Change = crate::procedure::NoChange;
        type Clock = crate::procedure::WallClock;
        type Config = ();
        fn name() -> &'static str {
            "poll_pool_gated_proc"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: usize,
            _: StateHandle<'_, Self>,
            _: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            while input == 0 && GATE_HELD.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            SERVED.fetch_add(1, Ordering::Release);
            Ok(())
        }
    }

    struct GatedOperative;
    impl StreamOperative for GatedOperative {
        fn factory_id() -> &'static str {
            "poll_pool_gated_operative"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<usize>("in").into::<GatedProc>();
        }
    }

    /// A private core with its own pool, so a case never observes (or
    /// disturbs) the process-global one. Leaked because a pool's threads
    /// outlive the case that started them.
    fn private_pool(base: usize, max: usize, idle_exit_ms: u64) -> Arc<PollPool> {
        let core: &'static PollCore = Box::leak(Box::new(PollCore::new()));
        let pool = Arc::new(PollPool {
            core,
            base,
            max,
            live: AtomicUsize::new(0),
            next_id: AtomicUsize::new(0),
            // The production streak is 30 s; what is under test is the
            // retirement, not its duration.
            idle_exit: Duration::from_millis(idle_exit_ms),
        });
        pool.start();
        pool
    }

    /// The whole point of the adaptive pool: with every base worker blocked
    /// in user code, queued work for another entity is still served — the
    /// core reports starvation, the supervisor answers with a thread — and
    /// the grown thread retires once the burst is over.
    #[test]
    fn a_starved_pool_grows_to_serve_the_queued_work_and_shrinks_back() {
        let _serialized = POOL_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        GATE_HELD.store(true, Ordering::Release);
        SERVED.store(0, Ordering::Release);
        let pool = private_pool(1, 4, 1_500);
        let (_ia, blocked) = build_slot_of::<GatedOperative>("poll-pool-blocked");
        let (_ib, healthy) = build_slot_of::<GatedOperative>("poll-pool-healthy");

        push_input(pool.core, &blocked, 0);
        wait_until("the base worker to enter user code", || {
            blocked.drain_active.load(Ordering::Acquire)
        });
        push_input(pool.core, &healthy, 1);

        wait_until("the queued work to be served while the gate holds", || {
            SERVED.load(Ordering::Acquire) >= 1
        });
        assert!(
            GATE_HELD.load(Ordering::Acquire),
            "the healthy entity was served only after the blocked one finished"
        );
        assert!(
            pool.live.load(Ordering::Acquire) > pool.base,
            "starvation did not grow the pool"
        );

        GATE_HELD.store(false, Ordering::Release);
        wait_until("the grown worker to retire", || {
            pool.live.load(Ordering::Acquire) == pool.base
        });
        pool.core.shutdown();
    }

    /// The accounting has to survive an unwind, because that is exactly the
    /// exit a trailing `fetch_sub` would skip: a worker gone but still counted
    /// keeps a slot in the ceiling and hides its own death from the supervisor.
    #[test]
    fn a_worker_that_unwinds_frees_its_slot() {
        let _serialized = POOL_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let pool = private_pool(1, 4, 300);
        wait_until("the base pool to come up", || {
            pool.live.load(Ordering::Acquire) == pool.base
        });

        let before = pool.live.load(Ordering::Acquire);
        let claim = Arc::clone(&pool);
        claim.live.fetch_add(1, Ordering::AcqRel);
        let died = std::thread::spawn(move || {
            let _slot = LiveWorker(claim);
            panic!("a poll worker died");
        })
        .join();

        assert!(died.is_err(), "the worker really did unwind");
        assert_eq!(
            pool.live.load(Ordering::Acquire),
            before,
            "an unwinding worker must release its pool slot"
        );
        pool.core.shutdown();
    }

    /// A shrunken pool that still keeps up starves nobody, so no starvation
    /// signal would ever arrive to replace a dead worker. The supervisor's
    /// second condition is what brings the pool back — and the replacement has
    /// to actually fetch.
    #[test]
    fn a_short_pool_is_topped_back_up_to_base_and_serves_again() {
        let _serialized = POOL_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        GATE_HELD.store(false, Ordering::Release);
        let pool = private_pool(2, 8, 300);
        wait_until("the base pool to come up", || {
            pool.live.load(Ordering::Acquire) == pool.base
        });

        // A worker died: its slot is free and the count is short.
        pool.live.fetch_sub(1, Ordering::AcqRel);
        wait_until("the supervisor to replace the dead worker", || {
            pool.live.load(Ordering::Acquire) == pool.base
        });

        let (_i, slot) = build_slot_of::<GatedOperative>("poll-pool-replaced");
        let before = SERVED.load(Ordering::Acquire);
        push_input(pool.core, &slot, 1);
        wait_until("the replacement worker to serve work", || {
            SERVED.load(Ordering::Acquire) > before
        });
        pool.core.shutdown();
    }

    /// Growth is measured against the LIVE count, so a departed worker frees
    /// the headroom it occupied instead of pushing the ceiling permanently
    /// out of reach.
    #[test]
    fn a_departed_worker_does_not_consume_the_ceiling() {
        let _serialized = POOL_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        GATE_HELD.store(false, Ordering::Release);
        let pool = private_pool(1, 2, 300);
        wait_until("the base pool to come up", || {
            pool.live.load(Ordering::Acquire) == pool.base
        });

        pool.grow();
        wait_until("the pool to reach its ceiling", || {
            pool.live.load(Ordering::Acquire) == pool.max
        });
        pool.grow();
        assert_eq!(
            pool.live.load(Ordering::Acquire),
            pool.max,
            "the ceiling must hold exactly"
        );

        // The grown worker retires; its slot returns to the ceiling's budget.
        wait_until("the grown worker to retire", || {
            pool.live.load(Ordering::Acquire) == pool.base
        });
        pool.grow();
        wait_until("growth to be possible again", || {
            pool.live.load(Ordering::Acquire) == pool.max
        });
        pool.core.shutdown();
    }

    /// A workload that never starves observes exactly the base pool: work is
    /// fed one round at a time, so a worker is always inside `poll` and the
    /// supervisor has nothing to answer.
    #[test]
    fn a_never_starved_run_keeps_exactly_the_base_pool() {
        let _serialized = POOL_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        GATE_HELD.store(false, Ordering::Release);
        let pool = private_pool(2, 8, 300);
        let (_i, slot) = build_slot_of::<GatedOperative>("poll-pool-steady");

        let before = SERVED.load(Ordering::Acquire);
        for i in 1..=20 {
            push_input(pool.core, &slot, i);
            wait_until("the round to complete", || {
                SERVED.load(Ordering::Acquire) >= before + i
            });
        }
        assert_eq!(
            pool.live.load(Ordering::Acquire),
            pool.base,
            "a healthy run grew threads it did not need"
        );
        pool.core.shutdown();
    }
}
