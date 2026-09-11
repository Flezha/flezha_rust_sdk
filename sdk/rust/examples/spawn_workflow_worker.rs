//! A stream detector that hands each interesting event to a short-lived
//! workflow worker (`procedure-requested-spawn`).
//!
//! The pattern this exists for: a stream operative notices something worth
//! doing real work about, and wants a worker for it — without an external
//! dispatcher, and without becoming one itself.
//!
//! `ctx.spawn_workflow` is spawn-or-deliver on one id: the first alert for
//! `ACME` starts `triage@ACME`, every later alert for `ACME` is fed to the
//! instance already working on it. Concurrent alerts resolve controller-side
//! to exactly one instance. The call is fire-and-forget — the detector's own
//! output is unaffected by whether the worker could be started.
//!
//! Run the pair with a runner that defines both:
//!     Flezha::create_runner("default")?
//!         .define_stream::<AlertDetector>()?
//!         .define_workflow::<Triage>()?
//!         .run()?;
//!
//! With authorization on, the default in a release, bind the namespace's runner
//! principal to `spawner` first, or every spawn resolves `Refused`:
//!     flz role bind runner@default spawner default
//! The role also lets this runner's `admin()` start and send to operatives in its
//! namespace, and nothing else.

use flezha_runner::procedure::{IfRunning, Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::{
    ChannelBuilder, StreamOperative, WorkflowCtx, WorkflowError, WorkflowOperative,
};

// -- the detector: stream, one instance, runs forever ---------------------

struct AlertDetector;

impl StreamOperative for AlertDetector {
    fn factory_id() -> &'static str {
        "alert_detector"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("events")
            .into::<Detect>()
            .named("alerts")
            .sink("alerts_sink");
    }
}

#[derive(Default)]
struct Detect;

impl Procedure for Detect {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "detect"
    }

    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}

    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let Some(subject) = input.strip_prefix("alert:") else {
            return Ok(());
        };

        // One worker per subject: the first alert spawns it with this payload
        // as its first input, later alerts are delivered to the same instance.
        // The handle is dropped — a failed request logs itself and this
        // procedure carries on emitting.
        let _ = ctx.spawn_workflow(
            format!("triage@{subject}"),
            "triage",
            input.as_bytes().to_vec(),
            IfRunning::Deliver,
        );

        ctx.emit(format!("detected:{subject}"))
    }
}

// -- the worker: workflow, one per subject, torn down when it returns ---------

struct Triage;

impl WorkflowOperative for Triage {
    fn factory_id() -> &'static str {
        "triage"
    }

    fn run(ctx: &mut WorkflowCtx, args: &str) -> Result<(), WorkflowError> {
        ctx.emit(format!("triaged:{args}").into_bytes())?;
        Ok(())
    }
}

fn main() {
    // Compile-only example: `cargo run --example spawn_workflow_worker` needs a
    // controller to connect to, so the runner wiring is documented above
    // rather than executed here.
    println!(
        "define {} and {} on one runner",
        AlertDetector::factory_id(),
        Triage::factory_id()
    );
}
