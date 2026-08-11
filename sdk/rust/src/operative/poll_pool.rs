//! The typed layer's poll workers: this SDK's half of the poll/complete seam.
//!
//! The core decides what runs and caps how much may be underway; a host's only
//! job is to bring the threads (`docs/sdk_poll_core.md`). Rust has no cheap
//! blocking, so this host answers with a pool that grows: a base set that
//! never retires, plus one supervisor parked in the core's starvation wait —
//! work queued with no thread inside `poll` at all. Each observation adds one
//! thread, up to a ceiling, and the threads above the base retire after an
//! idle streak, so a workload that never starves observes exactly the base
//! pool. (Java's counterpart hands each activation to a virtual thread and
//! never starves; the two arms share the core, not a mechanism.)

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use flz_core::entity::EntitySlot;

use crate::runner::poll_core::{global, PollCore, Polled, Starvation};

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

impl PollPool {
    fn from_env(core: &'static PollCore) -> Arc<Self> {
        let base = crate::runner::work_stealing_pool::configured_worker_count();
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

    /// Every observation means work is queued with nobody inside `poll` to
    /// take it, so the answer is one more thread. Growth is self-limiting
    /// without a settle here: the spawned worker's own `poll` entry restarts
    /// the core's dwell, so the next thread is at least a dwell away and only
    /// if the runner is still stuck.
    fn supervise(self: Arc<Self>) {
        loop {
            match self.core.wait_starvation(SUPERVISOR_WAIT) {
                Starvation::Observed => self.grow(),
                Starvation::Timeout => {}
                Starvation::ShuttingDown => return,
            }
        }
    }

    /// Add one worker if the ceiling allows. The slot is reserved before the
    /// spawn because a retiring worker may be decrementing concurrently, and
    /// the ceiling has to hold exactly.
    fn grow(self: &Arc<Self>) {
        if self
            .live
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |live| {
                (live < self.max).then_some(live + 1)
            })
            .is_err()
        {
            return;
        }
        if let Err(e) = self.spawn_worker(true) {
            self.live.fetch_sub(1, Ordering::AcqRel);
            tracing::warn!(
                target: "flezha::dispatch",
                error = %e,
                "starved runner could not grow its poll pool"
            );
        }
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
        self.live.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Start the process-wide pool (once): the host half of the poll/complete
/// seam. Called before the first input can be scheduled.
pub(crate) fn ensure_poll_workers() {
    static STARTED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    STARTED.get_or_init(|| {
        // A Block-policy producer facing a full consumer drains it on its own
        // thread through this hook (one-worker self-deadlock fix).
        flz_core::routing::install_inline_serve(serve_slot);
        PollPool::from_env(global()).start();
    });
}

/// Serve one slot inline: assemble its round on the calling thread and run
/// it. How a Block-policy producer drains a full consumer without waiting on
/// a poll worker (a parked worker could be the only one able to serve it).
pub(crate) fn serve_slot(slot: &Arc<EntitySlot>) {
    let core = global();
    if let Some(act) = core.try_assemble(slot) {
        super::dispatcher::run_activation(core, act);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::ChannelBuilder;
    use crate::entity::restore::RestoreInitPayload;
    use crate::operative::UnboundedOperative;
    use crate::procedure::state_handle::StateHandle;
    use crate::procedure::{Procedure, ProcedureCtx, ProcedureError};
    use crate::runner::poll_core_tests::fixtures::{build_slot_of, push_input};
    use std::sync::atomic::{AtomicBool, AtomicUsize};

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
                std::thread::sleep(Duration::from_millis(5));
            }
            SERVED.fetch_add(1, Ordering::Release);
            Ok(())
        }
    }

    struct GatedOperative;
    impl UnboundedOperative for GatedOperative {
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

    fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !done() {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
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
