//! Reshaping and keying on the wiring (`stream-transforms`).
//!
//! One trade feed, four candle timeframes, one candle Entity per
//! symbol-and-timeframe. Written with Procedures alone that is a filter class,
//! a fan-out class, and a keying hook buried inside the consumer — three places
//! to look and none of them the topology. Written on the edge it is four lines
//! you can read top to bottom:
//!
//! ```ignore
//! c.input::<Trade>("trades")
//!     .filter(|t: &Trade| t.qty > 0.0)          // no Procedure
//!     .flat_map(explode)                        // one trade → four slots
//!     .key_by(|s: &Slot| format!("{}@{}", s.symbol, s.tf))
//!     .into::<Candle>()                         // one Entity per slot
//!     .keyed()
//!     .output("candles");
//! ```
//!
//! The whole topology contains exactly ONE Procedure node. The transforms add
//! no Entity, no mailbox and no dispatch hop: everything declared before
//! `.into` composes into a single closure stored on that one consumer edge.
//! Fusion is structural — `.into` IS the boundary — so there is no optimizer to
//! second-guess and nothing to disable.
//!
//! Two obligations come with running on the edge, and they are the whole
//! contract:
//!
//! - **Pure.** Edge closures are evaluated at the routing seam, which live
//!   delivery and WAL replay both travel. A closure that read the clock would
//!   rebuild a state the live run never had.
//! - **Cheap.** They run on the routing thread. Real work belongs in the
//!   Procedure behind `key_by`, which is where per-Entity parallelism comes
//!   from. Reshaping a value is cheap; parsing a megabyte is not.
//!
//! `key_by` is **total**: it names an Entity, it never declines. Discarding an
//! input is `filter`'s job — which is why the qty check comes first.
//!
//! Run it with:
//!     Flezha::create_runner("default")?
//!         .define_unbounded::<Candles>()?
//!         .run()?;
//!
//! then feed it:
//!     flz operative send candles --channel trades --text '{"symbol":"AAPL","price":190.2,"qty":3.0}'

use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{Change, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::{ChannelBuilder, UnboundedOperative};
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

impl Change for Tick {}

/// The one Procedure in the graph. Note what it does NOT declare: no
/// `entity_keys()`, no keying member of any kind. It is dynamic-mode because a
/// keyed edge feeds it — and its Entities are created on first sight of each
/// key, exactly as the deleted `entity_key` hook produced.
#[derive(Default)]
struct Candle;

impl Procedure for Candle {
    type Input = Slot;
    type Output = String;
    type State = CandleState;
    type Change = Tick;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "candle"
    }

    fn init_state(_key: Option<&str>, _payload: Option<RestoreInitPayload<'_>>) -> CandleState {
        CandleState {
            high: f64::MIN,
            low: f64::MAX,
            trades: 0,
        }
    }

    fn apply_change(state: &mut CandleState, change: &Tick) {
        state.high = state.high.max(change.0);
        state.low = state.low.min(change.0);
        state.trades += 1;
    }

    fn run(
        &mut self,
        input: Slot,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(Tick(input.price));
        // The Entity IS the candle, so the emit key is the Entity's key.
        ctx.emit_keyed(
            format!("{}@{}", input.symbol, input.tf),
            format!(
                "{} {} h={} l={}",
                input.symbol, input.tf, state.high, state.low
            ),
        )
    }
}

struct Candles;

impl UnboundedOperative for Candles {
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
            .output("candles");
    }
}

fn main() {
    // Declaration-only example: `build_topology_for::<Candles>()` is what the
    // runner does at `define_unbounded`, and it is what proves the wiring above
    // is legal without needing a live controller.
    flezha_runner::operative::builder::build_topology_for::<Candles>()
        .expect("the candle topology builds");
    println!("candles topology builds: 1 procedure, 4 Entities per symbol");
}
