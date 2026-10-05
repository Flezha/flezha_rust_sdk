//! Reshaping and keying on the wiring.
//!
//! One trade feed, four candle timeframes, one candle Cell per
//! symbol-and-timeframe. Written with Tasklets alone that is a filter class
//! and a fan-out class in front of the consumer, two more places to look and
//! neither of them the topology. Written on the edge it is four lines you can
//! read top to bottom:
//!
//! ```ignore
//! c.input::<Trade>("trades")
//!     .filter(|t: &Trade| t.qty > 0.0)          // no Tasklet
//!     .flat_map(explode)                        // one trade → four slots
//!     .key_by(|s: &Slot| format!("{}@{}", s.symbol, s.tf))
//!     .into::<Candle>()                         // one Cell per slot
//!     .keyed()
//!     .output_named("candles");
//! ```
//!
//! The whole topology contains exactly ONE Tasklet node. The transforms add
//! no Cell, no mailbox and no dispatch hop: everything declared before
//! `.into` composes into a single closure stored on that one consumer edge.
//! Fusion is structural (`.into` IS the boundary), so there is no optimizer to
//! second-guess and nothing to disable.
//!
//! Two obligations come with running on the edge, and they are the whole
//! contract:
//!
//! - **Pure.** Edge closures are evaluated at the routing seam, which live
//!   delivery and WAL replay both travel. A closure that read the clock would
//!   rebuild a state the live run never had.
//! - **Cheap.** They run on the routing thread. Real work belongs in the
//!   Tasklet behind `key_by`, which is where per-Cell parallelism comes
//!   from. Reshaping a value is cheap; parsing a megabyte is not.
//!
//! `key_by` is **total**: it names a Cell, it never declines. Discarding an
//! input is `filter`'s job, which is why the qty check comes first.
//!
//! Run it with:
//!     Flezha::create_runner("default")?
//!         .register_factory(Candles)?
//!         .run()?;
//!
//! then feed it:
//!     flz relay send candles --channel trades --text '{"symbol":"BTC/USD","price":64100.0,"qty":0.5}'

use flezha_runner::state::Change;
use flezha_runner::tasklet::{Ctx, Tasklet, TaskletError};
use flezha_runner::{ChannelBuilder, Relay};
use serde::{Deserialize, Serialize};

/// The timeframes every trade is folded into.
const TIMEFRAMES: [&str; 4] = ["1m", "5m", "1h", "1d"];

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Trade {
    symbol: String,
    price: f64,
    qty: f64,
}

/// One trade, addressed to one timeframe's candle.
#[derive(Clone, Debug)]
struct Slot {
    symbol: String,
    tf: &'static str,
    price: f64,
}

/// One trade → one Slot per timeframe. Pure, and cheap.
fn explode(trade: Trade) -> Vec<Slot> {
    TIMEFRAMES
        .iter()
        .map(|tf| Slot {
            symbol: trade.symbol.clone(),
            tf,
            price: trade.price,
        })
        .collect()
}

/// The running candle for ONE symbol-and-timeframe.
#[derive(Default, Serialize, Deserialize)]
struct CandleState {
    high: f64,
    low: f64,
    trades: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct Tick(f64);

impl Change<CandleState> for Tick {
    fn apply_to(&self, state: &mut CandleState) {
        state.high = state.high.max(self.0);
        state.low = state.low.min(self.0);
        state.trades += 1;
    }
}

/// The one Tasklet in the graph. Note what it does NOT declare: no
/// `cell_keys()`, no keying member of any kind. It is dynamic-mode because a
/// keyed edge feeds it, and its Cells are created on first sight of each
/// key.
#[derive(Default)]
struct Candle;

#[flezha_runner::tasklet(name = "candle")]
impl Tasklet for Candle {
    type Input = Slot;
    type Output = String;
    type State = CandleState;
    type Change = Tick;

    fn init_state(_key: Option<&str>) -> CandleState {
        CandleState {
            high: f64::MIN,
            low: f64::MAX,
            trades: 0,
        }
    }

    fn run(&mut self, input: Slot, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Tick(input.price));
        // The Cell IS the candle, so the plain emit already carries the
        // Cell's own key (`key_by`'s value) and cannot drift from it.
        // `emit_keyed` is for the output whose key is a different one.
        ctx.emit(format!(
            "{} {} h={} l={}",
            input.symbol,
            input.tf,
            ctx.state().high,
            ctx.state().low
        ))
    }
}

struct Candles;

impl Relay for Candles {
    fn factory_id() -> &'static str {
        "candles"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<Trade>("trades")
            .filter(|t: &Trade| t.qty > 0.0)
            .flat_map(explode)
            .key_by(|s: &Slot| format!("{}@{}", s.symbol, s.tf))
            .into::<Candle>()
            .keyed()
            .output_named("candles");
    }
}

fn main() {
    // Declaration-only example: `build_topology_for::<Candles>()` is what the
    // runner does at `register_factory`, and it is what proves the wiring above
    // is legal without needing a live controller.
    flezha_runner::relay::builder::build_topology_for::<Candles>()
        .expect("the candle topology builds");
    println!("candles topology builds: 1 tasklet, 4 Cells per symbol");
}
