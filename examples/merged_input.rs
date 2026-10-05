//! A detector fed by both its data feed and a live config channel.
//!
//! The pattern this exists for: a tasklet whose behaviour must change while
//! it runs. Without a merge the only ways to reconfigure are to restart the
//! relay with new env, or to bolt the config onto every data payload ,
//! the first loses the state you were accumulating, the second couples the
//! producer to the consumer's knobs.
//!
//! `c.merge::<I>()` declares the tasklet's `Input` as one sum type and gives
//! each source channel a mapping into it. Everything downstream , `run`,
//! `cells_for`, state, emit , sees exactly one type, so nothing else about
//! the tasklet changes.
//!
//! Two things worth knowing before you reach for it:
//!
//! - **Per-source FIFO only.** Readings arrive in order and thresholds arrive
//!   in order, but nothing orders one against the other. Correlate in state,
//!   as `Detector` does by simply remembering the last threshold it saw.
//! - **The mappings must be pure.** They run again on WAL replay, and a replay
//!   that mapped differently would rebuild a state the live run never had.
//!
//! Run it with:
//!     Flezha::create_runner("default")?
//!         .register_factory(Thermostat)?
//!         .run()?;
//!
//! then feed the two channels independently:
//!     flz relay send thermostat --channel readings --text '{"sensor":"kiln","celsius":812.0}'
//!     flz relay send thermostat --channel config   --text '{"max_celsius":800.0}'

use flezha_runner::{tasklet, Change, ChannelBuilder, Ctx, Relay, Tasklet, TaskletError};
use serde::{Deserialize, Serialize};

// -- the two feeds ----------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Reading {
    sensor: String,
    celsius: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Threshold {
    max_celsius: f64,
}

/// The merged input: one sum type over everything the detector consumes. Its
/// variants are the mappings , `DetectorInput::Reading` is already an
/// `Fn(Reading) -> DetectorInput`, so the declaration below reads as the wiring
/// diagram it is.
#[derive(Clone, Debug)]
enum DetectorInput {
    Reading(Reading),
    Threshold(Threshold),
}

// -- the detector -----------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DetectorState {
    max_celsius: f64,
    alerts: u64,
}

impl Default for DetectorState {
    fn default() -> Self {
        // The safe default until the config channel says otherwise: an
        // relay that starts before its config must not alert on everything.
        Self {
            max_celsius: f64::INFINITY,
            alerts: 0,
        }
    }
}

/// What the detector records: a new threshold, or one more alert.
#[derive(Serialize, Deserialize)]
enum DetectorChange {
    Threshold(f64),
    Alerted,
}

impl Change<DetectorState> for DetectorChange {
    fn apply_to(&self, state: &mut DetectorState) {
        match self {
            DetectorChange::Threshold(max) => state.max_celsius = *max,
            DetectorChange::Alerted => state.alerts += 1,
        }
    }
}

#[derive(Default)]
struct Detector;

#[tasklet(name = "detector")]
impl Tasklet for Detector {
    type Input = DetectorInput;
    type Output = String;
    type State = DetectorState;
    type Change = DetectorChange;

    fn run(&mut self, input: DetectorInput, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        match input {
            // A config frame is an ordinary input: it lands in the same
            // durable state as the readings, so a restart resumes with the
            // threshold that was in force.
            DetectorInput::Threshold(t) => ctx.change(DetectorChange::Threshold(t.max_celsius)),
            DetectorInput::Reading(r) if r.celsius > ctx.state().max_celsius => {
                ctx.change(DetectorChange::Alerted);
                ctx.emit(format!("{} over threshold at {:.1}C", r.sensor, r.celsius))?;
            }
            DetectorInput::Reading(_) => {}
        }
        Ok(())
    }
}

// -- the relay ----------------------------------------------------------

struct Thermostat;

impl Relay for Thermostat {
    fn factory_id() -> &'static str {
        "thermostat"
    }

    fn channels(c: &ChannelBuilder) {
        // Two entry channels, one consumer. Either source may equally be
        // another tasklet's output stream , `.from` takes both.
        c.merge::<DetectorInput>()
            .from(c.input::<Reading>("readings"), DetectorInput::Reading)
            .from(c.input::<Threshold>("config"), DetectorInput::Threshold)
            .into::<Detector>()
            .sink_named("alerts");
    }
}

fn main() {
    // Declaration-only example: `cargo run --example merged_input` builds the
    // topology and prints its shape rather than dialing a controller.
    match flezha_runner::relay::builder::build_topology_for::<Thermostat>() {
        Ok(topology) => {
            println!("relay `{}`", topology.factory_id);
            for channel in &topology.channels {
                println!(
                    "  channel `{}` ({:?}) -> {} consumer(s)",
                    channel.name,
                    channel.kind,
                    channel.consumers.len()
                );
            }
        }
        Err(e) => eprintln!("topology refused: {e}"),
    }
}
