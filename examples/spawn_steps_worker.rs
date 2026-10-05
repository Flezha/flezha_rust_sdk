//! A stream detector that hands each interesting event to a short-lived
//! steps worker.
//!
//! The pattern this exists for: a flow relay notices something worth
//! doing real work about, and wants a worker for it, without an external
//! dispatcher, and without becoming one itself.
//!
//! `ctx.spawn_steps_with` under an id is spawn-or-deliver: the first alert
//! for `ACME` starts `triage@ACME`, every later alert for `ACME` is fed to the
//! instance already working on it. Concurrent alerts resolve controller-side
//! to exactly one instance. `ctx.spawn_steps` without an id starts a new
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
//! The role also lets this runner's `control()` start relays and its `send`
//! reach relays in its namespace, and nothing else.

use flezha_runner::tasklet::{IfRunning, SpawnOptions};
use flezha_runner::{
    tasklet, ChannelBuilder, Ctx, Each, OnFailure, Relay, RelaySettings, Run, Tasklet, TaskletError,
};

// -- the detector: stream, one instance, runs forever ---------------------

struct AlertDetector;

impl Relay for AlertDetector {
    fn factory_id() -> &'static str {
        "alert_detector"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("events")
            .into::<Detect>()
            .sink_named("alerts");
    }
}

#[derive(Default)]
struct Detect;

#[tasklet(name = "detect")]
impl Tasklet for Detect {
    type Input = String;
    type Output = String;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let Some(subject) = input.strip_prefix("alert:") else {
            return Ok(());
        };

        // One worker per subject: the first alert spawns it with this payload
        // as its first input, later alerts are delivered to the same instance.
        // The handle is dropped: a failed request logs itself and this
        // tasklet carries on emitting.
        let _ = ctx.spawn_steps_with(
            "triage",
            input.as_bytes().to_vec(),
            SpawnOptions::id(format!("triage@{subject}")).if_running(IfRunning::Deliver),
        );

        ctx.emit(format!("detected:{subject}"))
    }
}

// -- the worker: steps relay, one per subject, torn down when its pass ends -----

struct Triage;

impl Relay for Triage {
    fn factory_id() -> &'static str {
        "triage"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("alert")
            .into::<TriageStep>()
            .sink_named("triaged");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            ..RelaySettings::default()
        }
    }
}

#[derive(Default)]
struct TriageStep;

#[tasklet(name = "triage_step")]
impl Tasklet for TriageStep {
    type Input = String;
    type Output = String;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("triaged:{input}"))
    }
}

fn main() {
    // Compile-only example: `cargo run --example spawn_steps_worker` needs a
    // controller to connect to, so the runner wiring is documented above
    // rather than executed here.
    println!(
        "register {} and {} on one runner",
        AlertDetector::factory_id(),
        Triage::factory_id()
    );
}
