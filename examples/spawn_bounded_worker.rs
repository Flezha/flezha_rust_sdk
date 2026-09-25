//! A stream detector that hands each interesting event to a short-lived
//! bounded worker (`tasklet-requested-spawn`).
//!
//! The pattern this exists for: an unbounded operator notices something worth
//! doing real work about, and wants a worker for it — without an external
//! dispatcher, and without becoming one itself.
//!
//! `ctx.spawn_bounded_with` under an id is spawn-or-deliver: the first alert
//! for `ACME` starts `triage@ACME`, every later alert for `ACME` is fed to the
//! instance already working on it. Concurrent alerts resolve controller-side
//! to exactly one instance. `ctx.spawn_bounded` without an id starts a new
//! instance every time under a generated id, which suits a job that runs once
//! and ends. Either call is fire-and-forget: the detector's own output is
//! unaffected by whether the worker could be started.
//!
//! Run the pair with a runner that registers both:
//!     Flezha::create_runner("default")?
//!         .register_factory(AlertDetector)?
//!         .register_factory(Triage)?
//!         .run()?;
//!
//! With authorization on, the default in a release, bind the namespace's runner
//! principal to `spawner` first, or every spawn resolves `Refused`:
//!     flz role bind runner@default spawner default
//! The role also lets this runner's `admin()` start and send to operators in its
//! namespace, and nothing else.

use flezha_runner::tasklet::{IfRunning, SpawnOptions, TaskletError};
use flezha_runner::{tasklet, BoundedOperator, ChannelBuilder, UnboundedOperator};

// -- the detector: stream, one instance, runs forever ---------------------

struct AlertDetector;

impl UnboundedOperator for AlertDetector {
    fn factory_id() -> &'static str {
        "alert_detector"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("events")
            .into::<Detect>()
            .sink_named("alerts");
    }
}

#[tasklet(id = "detect", emits = String)]
fn detect(input: String, ctx: &mut flezha_runner::TaskletCtx<'_>) -> Result<(), TaskletError> {
    let Some(subject) = input.strip_prefix("alert:") else {
        return Ok(());
    };

    // One worker per subject: the first alert spawns it with this payload
    // as its first input, later alerts are delivered to the same instance.
    // The handle is dropped: a failed request logs itself and this
    // tasklet carries on emitting.
    let _ = ctx.spawn_bounded_with(
        "triage",
        input.as_bytes().to_vec(),
        SpawnOptions::id(format!("triage@{subject}")).if_running(IfRunning::Deliver),
    );

    ctx.emit(format!("detected:{subject}"))
}

// -- the worker: bounded operator, one per subject, torn down when its pass ends -----

struct Triage;

impl BoundedOperator for Triage {
    fn factory_id() -> &'static str {
        "triage"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("alert")
            .into::<TriageStep>()
            .sink_named("triaged");
    }
}

#[tasklet(id = "triage_step")]
fn triage_step(input: String) -> String {
    format!("triaged:{input}")
}

fn main() {
    // Compile-only example: `cargo run --example spawn_bounded_worker` needs a
    // controller to connect to, so the runner wiring is documented above
    // rather than executed here.
    println!(
        "register {} and {} on one runner",
        AlertDetector::factory_id(),
        Triage::factory_id()
    );
}
