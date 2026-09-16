//! Instance-count scaling probe for the workflow driver.
//!
//! One driver thread runs per actively-running workflow instance. This probe
//! measures what that costs at population: resident memory per instance, the
//! CPU a *blocked* population still burns, how long starting and tearing one
//! down takes, and whether an unrelated instance beside it still completes at
//! its unloaded latency.
//!
//! A `harness = false` bench, not a test: it spawns thousands of threads and
//! takes tens of seconds, and it reports numbers rather than asserting a
//! contract. One configuration per process, all of it read from the
//! environment, so a run is reproducible and every delta is measured against a
//! process that has done nothing else:
//!
//! ```text
//! FLEZHA_SCALE_N=10000 FLEZHA_SCALE_SHAPE=step \
//!   FLEZHA_WORKFLOW_DRIVER_STACK_KIB=256 \
//!   cargo bench -p flezha_runner --features bench-internals \
//!     --bench workflow_scaling
//! ```
//!
//! Classes are declared through the ordinary authoring path
//! (`Flezha::create_runner(..).define_workflow::<T>()`, which registers without
//! connecting to anything) and driven through a `RunnerNode`, so the probe
//! measures what an operator's runner does rather than a test back door. The
//! SDK's poll workers spawn themselves on the first instance's entity build,
//! which is why `FLEZHA_WORKER_POOL_SIZE` is set before the first start.
//!
//! Two population shapes, because a blocked instance does not always cost the
//! same number of threads:
//!
//! - `step` — blocked mid-step, the agent-turn shape (a 30 s model call). Costs
//!   a driver thread AND the SDK worker running the step. Java's worker is a
//!   virtual thread, so there an agent turn costs one OS thread; Rust's is a
//!   pool thread, so there it costs two.
//! - `idle` — blocked in the inbox wait of a `PerInput` loop. Costs the driver
//!   thread alone, which is what isolates the per-driver number the gate
//!   criterion is stated in.

// The suite's one polling primitive, reached from `benches/` rather than
// copied: charter §9 allows exactly one `wait_until` per layer.
#[path = "../tests/common/mod.rs"]
mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use common::wait_until;
use crossbeam::channel::Receiver;
use flezha_runner::channel::ChannelBuilder;
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError, StateHandle};
use flezha_runner::state::{NoChange, RestoreInitPayload, WallClock};
use flezha_runner::{Flezha, LoopMode, WorkflowOperative};
use flz_core::runner::{install_reconnect_trigger, RunnerMessage, RunnerNode};
use flz_core::util::env_parse;

/// The gate the population parks on, opened once the hold window has elapsed.
/// A condvar rather than a polled flag on purpose: a polling step would burn
/// CPU the probe would then attribute to the blocked population, which is one
/// of the numbers being measured.
static GATE: (Mutex<bool>, Condvar) = (Mutex::new(false), Condvar::new());
/// Steps currently parked on the gate — how the probe knows the population is
/// actually blocked rather than still starting.
static AT_GATE: AtomicUsize = AtomicUsize::new(0);

/// The blocked population's procedure: parks on the gate for as long as the
/// probe holds it, the way an agent turn parks on a model call.
#[derive(Default)]
struct GateProc;
impl Procedure for GateProc {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "scale_gate"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _input: String,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let (lock, opened) = &GATE;
        let mut open = lock.lock().unwrap_or_else(|p| p.into_inner());
        AT_GATE.fetch_add(1, Ordering::SeqCst);
        while !*open {
            open = opened.wait(open).unwrap_or_else(|p| p.into_inner());
        }
        AT_GATE.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}

/// The control instance's procedure, and the idle population's: as short as a
/// step gets, so what the control's completion latency measures is the path —
/// driver spawn, step handout, barrier, teardown — and not the body.
#[derive(Default)]
struct ShortProc;
impl Procedure for ShortProc {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "scale_short"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(input)?;
        Ok(())
    }
}

/// Blocked mid-step: a driver parked at its stage barrier while the SDK's
/// worker runs the gated body. `FLEZHA_SCALE_MAX_DURATION_MS` adds the
/// duration backstop a real agent class would declare — read at registration,
/// which is where `build_spec` asks — because that backstop is a third thread
/// per run (`spawn_duration_watcher`), and whether it is armed is exactly the
/// difference between two and three OS threads per blocked turn.
struct ScaleStepWorkflow;
impl WorkflowOperative for ScaleStepWorkflow {
    fn factory_id() -> &'static str {
        "scale-step-workflow"
    }
    const PERSISTENCE_ENABLED: bool = false;
    fn max_duration() -> Option<Duration> {
        match env_parse(ENV_MAX_DURATION_MS, 0u64) {
            0 => None,
            ms => Some(Duration::from_millis(ms)),
        }
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in").into::<GateProc>().named("out");
    }
}

/// Blocked in the inbox wait: a `PerInput` loop with nothing to consume. The
/// driver thread is the only thread the instance holds — no worker (no step is
/// running) and no duration watcher — which is what isolates the per-driver
/// cost. It is `self_terminating` in the declared sense: the loop ends when
/// its inbox closes.
struct ScaleIdleWorkflow;
impl WorkflowOperative for ScaleIdleWorkflow {
    fn factory_id() -> &'static str {
        "scale-idle-workflow"
    }
    const PERSISTENCE_ENABLED: bool = false;
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in").into::<ShortProc>().named("out");
    }
}

/// The instance measured beside the population.
struct ScaleControlWorkflow;
impl WorkflowOperative for ScaleControlWorkflow {
    fn factory_id() -> &'static str {
        "scale-control-workflow"
    }
    const PERSISTENCE_ENABLED: bool = false;
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ShortProc>()
            .sink_named("out");
    }
}

const ENV_N: &str = "FLEZHA_SCALE_N";
const ENV_SHAPE: &str = "FLEZHA_SCALE_SHAPE";
const ENV_HOLD_MS: &str = "FLEZHA_SCALE_HOLD_MS";
const ENV_POOL: &str = "FLEZHA_SCALE_POOL";
const ENV_MAX_DURATION_MS: &str = "FLEZHA_SCALE_MAX_DURATION_MS";
const ENV_DRIVER_STACK_KIB: &str = "FLEZHA_WORKFLOW_DRIVER_STACK_KIB";

/// How long the unloaded baseline, the discarded warmup, and each quiet-CPU
/// window run for. Short on purpose — the population, not the baseline, is
/// what the run is spending its time on.
const BASELINE_WINDOW: Duration = Duration::from_secs(2);
/// Workers kept free of the population, so the control instance always has one.
const SPARE_WORKERS: usize = 8;
/// How long one control instance may take before the probe calls it wedged.
const CONTROL_DEADLINE: Duration = Duration::from_secs(60);
/// How long the population may take to reach fully-blocked, and to unwind
/// again once the gate opens. Generous: both are linear in N, and a probe that
/// gave up early would report a number for a population it never assembled.
const SETTLE_DEADLINE: Duration = Duration::from_secs(120);
const DRAIN_DEADLINE: Duration = Duration::from_secs(180);

/// Which wait the population is blocked in, and therefore how many threads
/// each instance holds.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// Mid-step, on the gate: driver + SDK worker.
    Step,
    /// In the inbox wait of a `PerInput` loop: driver only.
    Idle,
}

impl Shape {
    fn from_env() -> Self {
        match std::env::var(ENV_SHAPE).as_deref() {
            Ok("idle") => Shape::Idle,
            _ => Shape::Step,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Shape::Step => "step",
            Shape::Idle => "idle",
        }
    }

    fn factory_id(self) -> &'static str {
        match self {
            Shape::Step => ScaleStepWorkflow::factory_id(),
            Shape::Idle => ScaleIdleWorkflow::factory_id(),
        }
    }

    /// How many SDK workers the pool needs: one per instance for a population
    /// that holds a worker each, plus the spares that keep the control
    /// instance servable; only the spares otherwise.
    fn workers_for(self, n: usize) -> usize {
        match self {
            Shape::Step => n + SPARE_WORKERS,
            Shape::Idle => SPARE_WORKERS,
        }
    }

    /// Whether the whole population has reached its wait. A gated step counts
    /// itself in; an inbox wait leaves no such mark, so its drivers are counted
    /// as threads the process did not have before.
    fn all_blocked(self, n: usize, threads_before: u64) -> bool {
        match self {
            Shape::Step => AT_GATE.load(Ordering::SeqCst) >= n,
            Shape::Idle => thread_count() >= threads_before + n as u64,
        }
    }
}

/// One `/proc/self/status` field, in the units the kernel prints it.
fn status_field(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix(field)?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
        })
        .unwrap_or(0)
}

fn rss_kib() -> u64 {
    status_field("VmRSS:")
}

/// Virtual size. Not a cost in itself — but it is the only place a driver's
/// stack *size* shows up at all, so it is what proves the stack knob took
/// effect when the resident number does not move. Read it only at populations
/// in the thousands: glibc maps a fresh 64 MiB malloc arena per thread up to
/// 8× the core count, so at small N those few arenas dominate the delta and
/// the per-instance figure is meaningless.
fn vsize_kib() -> u64 {
    status_field("VmSize:")
}

fn thread_count() -> u64 {
    status_field("Threads:")
}

/// User + system CPU this process has consumed, in milliseconds. The field
/// walk starts after the parenthesised `comm`, which may itself contain spaces
/// and parentheses — hence the split on the LAST `)`.
fn cpu_ms() -> u64 {
    const TICKS_PER_SEC: u64 = 100;
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return 0;
    };
    let Some((_, after_comm)) = stat.rsplit_once(')') else {
        return 0;
    };
    let fields: Vec<&str> = after_comm.split_whitespace().collect();
    // After the comm, field 0 is `state`; utime and stime are 11 and 12.
    let ticks = |idx: usize| -> u64 { fields.get(idx).and_then(|v| v.parse().ok()).unwrap_or(0) };
    (ticks(11) + ticks(12)) * 1000 / TICKS_PER_SEC
}

/// One control run: start a fresh single-pass instance and wait for its
/// completion. Returns the round-trip.
fn control_sample(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>, seq: usize) -> Duration {
    let id = format!("scale-control-{seq}");
    let started = Instant::now();
    assert!(
        node.start_workflow(
            ScaleControlWorkflow::factory_id().into(),
            id.clone(),
            "ping".into()
        ),
        "control instance {id} failed to start"
    );
    let deadline = Instant::now() + CONTROL_DEADLINE;
    loop {
        let msg = rx
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap_or_else(|_| panic!("control instance {id} never completed"));
        if let RunnerMessage::EmitWorkflowCompleted {
            operative_id,
            status,
            ..
        } = msg
        {
            if operative_id == id {
                assert_eq!(status, "ok", "control instance {id} completed abnormally");
                // The registry entry outlives the run; drop it so a long
                // sampling window does not accumulate one per sample.
                node.stop_operative(&id);
                return started.elapsed();
            }
        }
    }
}

/// Nearest-rank percentile of a sample set (sorted in place).
fn percentile(samples: &mut [Duration], pct: f64) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    samples.sort_unstable();
    let rank = ((pct / 100.0) * samples.len() as f64).ceil() as usize;
    samples[rank.clamp(1, samples.len()) - 1]
}

/// Sample the control instance for `window`, returning every round-trip.
fn control_window(
    node: &mut RunnerNode,
    rx: &Receiver<RunnerMessage>,
    seq: &mut usize,
    window: Duration,
) -> Vec<Duration> {
    let until = Instant::now() + window;
    let mut samples = Vec::new();
    while Instant::now() < until {
        *seq += 1;
        samples.push(control_sample(node, rx, *seq));
    }
    samples
}

/// CPU burned over a window in which this thread does nothing, as a percentage
/// of one core. Measured with the control loop stopped: a sampling loop that
/// starts and tears down thousands of instances per second dwarfs whatever the
/// blocked population costs, and would be attributed to it.
fn quiet_cpu_pct(window: Duration) -> f64 {
    let before = cpu_ms();
    let began = Instant::now();
    std::thread::sleep(window);
    let elapsed = began.elapsed();
    100.0 * cpu_ms().saturating_sub(before) as f64 / elapsed.as_millis().max(1) as f64
}

fn main() {
    workflow_instance_scaling_probe();
}

fn workflow_instance_scaling_probe() {
    let n = env_parse(ENV_N, 1_000usize);
    let shape = Shape::from_env();
    let hold = Duration::from_millis(env_parse(ENV_HOLD_MS, 15_000u64));
    let stack = std::env::var(ENV_DRIVER_STACK_KIB).unwrap_or_else(|_| "default".into());
    let watchdog = env_parse(ENV_MAX_DURATION_MS, 0u64);

    // A blocked `step` population holds one SDK worker each, so the pool has to
    // be able to run the whole population plus the control instance beside it;
    // an `idle` one holds none, and needs only enough to serve the control
    // instance. Pinned in both shapes rather than left to the host's core count
    // so the two are comparable and the reported number is the applied one. The
    // workers spawn on the first instance's entity build, so this has to be set
    // before the first start. `FLEZHA_SCALE_POOL` decouples pool size from N,
    // which is how the probe tells a cost of the blocked population apart from
    // a cost of the pool sized for it.
    let pool = env_parse(ENV_POOL, shape.workers_for(n));
    std::env::set_var("FLEZHA_WORKER_POOL_SIZE", pool.to_string());

    let _runner_guard = flz_core::runner::env_test_guard();
    let (tx, rx) = crossbeam::channel::unbounded::<RunnerMessage>();
    install_reconnect_trigger(tx);

    // Declaring registers the classes; a runner that is never started never
    // dials a controller, which is all this probe needs from it.
    let _runner = Flezha::create_runner("scale-ns")
        .and_then(|r| {
            r.define_workflow::<ScaleStepWorkflow>()?
                .define_workflow::<ScaleIdleWorkflow>()?
                .define_workflow::<ScaleControlWorkflow>()?;
            Ok(r)
        })
        .expect("declaring the probe classes");

    let mut node = RunnerNode::new("scale-ns".to_string());
    let mut control_seq = 0usize;

    // Discarded: first-touch faults and allocator warmup land here instead of
    // in the baseline the loaded numbers are compared against. This is also
    // what spawns the poll workers.
    control_window(&mut node, &rx, &mut control_seq, BASELINE_WINDOW);

    // Unloaded baseline, on this same warmed process before the population
    // exists — the only comparison that isolates the population's effect.
    let base_cpu_pct = quiet_cpu_pct(BASELINE_WINDOW);
    let mut baseline = control_window(&mut node, &rx, &mut control_seq, BASELINE_WINDOW);
    let baseline_p50 = percentile(&mut baseline, 50.0);
    let baseline_p99 = percentile(&mut baseline, 99.0);
    let baseline_rate = baseline.len() as f64 / BASELINE_WINDOW.as_secs_f64();

    let before_rss = rss_kib();
    let before_vsize = vsize_kib();
    let before_threads = thread_count();

    // --- start the population -------------------------------------------------
    let start_began = Instant::now();
    for i in 0..n {
        assert!(
            node.start_workflow(
                shape.factory_id().into(),
                format!("scale-{i}"),
                "work".into()
            ),
            "population instance {i} failed to start"
        );
    }
    let start_elapsed = start_began.elapsed();

    // Blocked means blocked: nothing is measured until every instance has
    // reached its wait.
    let settled = wait_until(|| shape.all_blocked(n, before_threads), SETTLE_DEADLINE);
    assert!(
        settled,
        "population never reached {n} blocked instances (at gate {}, threads {})",
        AT_GATE.load(Ordering::SeqCst),
        thread_count()
    );

    let loaded_rss = rss_kib();
    let loaded_vsize = vsize_kib();
    let loaded_threads = thread_count();

    // --- the population is blocked: measure beside it -------------------------
    // What a blocked population costs while nothing else runs, then how an
    // unrelated instance fares beside it.
    let loaded_cpu_pct = quiet_cpu_pct(BASELINE_WINDOW);
    let mut loaded = control_window(&mut node, &rx, &mut control_seq, hold);
    let loaded_p50 = percentile(&mut loaded, 50.0);
    let loaded_p99 = percentile(&mut loaded, 99.0);
    let loaded_rate = loaded.len() as f64 / hold.as_secs_f64();

    // --- release and tear down ------------------------------------------------
    let teardown_began = Instant::now();
    {
        let (lock, opened) = &GATE;
        *lock.lock().unwrap_or_else(|p| p.into_inner()) = true;
        opened.notify_all();
    }
    for i in 0..n {
        node.stop_operative(&format!("scale-{i}"));
    }
    let drained = wait_until(
        || thread_count() <= before_threads + SPARE_WORKERS as u64,
        DRAIN_DEADLINE,
    );
    let teardown_elapsed = teardown_began.elapsed();

    let rss_per = loaded_rss.saturating_sub(before_rss) as f64 / n as f64;
    let vsize_per = loaded_vsize.saturating_sub(before_vsize) as f64 / n as f64;
    let threads_per = loaded_threads.saturating_sub(before_threads) as f64 / n as f64;
    let p99_ratio = loaded_p99.as_secs_f64() / baseline_p99.as_secs_f64().max(f64::EPSILON);
    let shape = shape.name();

    println!("--- workflow instance scaling probe -------------------------------");
    println!(
        "shape={shape} n={n} driver_stack_kib={stack} hold={hold:?} max_duration_ms={watchdog}"
    );
    println!(
        "threads: {before_threads} (incl. {pool} pre-sized SDK workers) -> {loaded_threads} \
         ({threads_per:.2} per instance){}",
        if drained { "" } else { " [DID NOT DRAIN]" }
    );
    println!(
        "rss: {before_rss} KiB -> {loaded_rss} KiB ({rss_per:.1} KiB per instance) · \
         vsize: {before_vsize} KiB -> {loaded_vsize} KiB ({vsize_per:.0} KiB per instance)"
    );
    println!(
        "cpu with nothing to do: {base_cpu_pct:.1}% of one core unloaded -> \
         {loaded_cpu_pct:.1}% with the population blocked"
    );
    println!("start: {start_elapsed:?} for {n} · teardown: {teardown_elapsed:?}");
    println!(
        "control p50 {baseline_p50:?} -> {loaded_p50:?} · \
         p99 {baseline_p99:?} -> {loaded_p99:?} ({p99_ratio:.2}x) · \
         rate {baseline_rate:.0}/s -> {loaded_rate:.0}/s"
    );
    println!(
        "PROBE\tshape={shape}\tn={n}\tstack={stack}\twatchdog_ms={watchdog}\tpool={pool}\t\
         rss_kib_per_instance={rss_per:.1}\tvsize_kib_per_instance={vsize_per:.0}\t\
         threads_per_instance={threads_per:.2}\tcpu_pct_base={base_cpu_pct:.1}\t\
         cpu_pct_loaded={loaded_cpu_pct:.1}\tstart_ms={}\tteardown_ms={}\t\
         p99_ratio={p99_ratio:.2}\trate_base={baseline_rate:.0}\trate_loaded={loaded_rate:.0}",
        start_elapsed.as_millis(),
        teardown_elapsed.as_millis(),
    );
}
