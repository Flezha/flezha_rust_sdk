//! Reading a Cell's live state with `flz state get`
//! (`cell-state-live-query`).
//!
//! The pattern this exists for: something outside the pipeline needs the
//! CURRENT value of one key — a dashboard, an ops question, a health check.
//! `flz state at` serves the controller's WAL at a point in the PAST, so
//! "what is BTCUSD doing right now" is a question for `flz state get`, which
//! reads the Cell itself rather than a copy that reached a sink.
//!
//! **This example declares nothing special.** That is the point: every Cell
//! of a running Operator is readable, because the read reuses the
//! `CellState` command the SDK answers for migration. Declaring
//! `type Change` is what makes the value durable; nothing extra makes it
//! readable.
//!
//! Run the operator:
//!     Flezha::create_runner("default")?
//!         .register_factory(Market)?
//!         .run()?;
//!
//! feed it a couple of trades:
//!     flz operator send market --channel trades --text '{"symbol":"BTCUSD","price":64100.0,"qty":0.5}'
//!     flz operator send market --channel trades --text '{"symbol":"ETHUSD","price":3120.0,"qty":4.0}'
//!
//! then read ONE Cell by name:
//!     $ flz state get market/candle/BTCUSD
//!     state get market/candle/BTCUSD:
//!       change_seq: 1
//!       state:      {"candle":{"BTCUSD":{"high":64100.0,"volume":0.5,"trades":1}}}
//!
//! The `change_seq` is the version the state was serialized at. It may be
//! AHEAD of what is durable: the read takes the Cell's state lock and does
//! not drain pending changes, so a change `run` applied a millisecond ago is in
//! the answer whether or not the flusher has shipped it yet. That is the
//! contract — freshest value, version reported — and it is why a read triggers
//! no compaction, no snapshot and no WAL truncation.
//!
//! Narrowing costs what it asks for. Drop the key to read the whole Tasklet
//! (no single version, so no seq):
//!     $ flz state get market/candle --output json
//!     {"change_seq":null,"path":"market/candle","state":{"candle":{"BTCUSD":{...},"ETHUSD":{...}}}}
//!
//! A Cell nothing holds is REFUSED, naming which case and pointing at the
//! durable path — it is never quietly served from disk:
//!     $ flz state get market/candle/DOGEUSD
//!     state get: cell_not_found: tasklet `candle` holds no Cell keyed
//!     `DOGEUSD` — read its durable state with `flz state at`
//!
//! and, once this operator's 10-minute idle TTL has evicted a quiet symbol:
//!     $ flz state get market/candle/ETHUSD
//!     state get: cell_evicted: Cell `ETHUSD` of tasklet `candle` was
//!     idle-evicted; eviction is destructive, so there is no durable value to
//!     serve — read its durable state with `flz state at`
//!
//! Both exit non-zero. `cell_evicted` and `cell_not_found` are different
//! diagnoses on purpose: eviction unlinks the Cell's files, so after the fact
//! only the SDK's bounded operator eviction trace can tell you the key ever existed.

use std::time::Duration;

use flezha_runner::state::{Change, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::tasklet::{Tasklet, TaskletCtx, TaskletError};
use flezha_runner::{ChannelBuilder, UnboundedOperator};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Trade {
    symbol: String,
    price: f64,
    qty: f64,
}

/// What a live read returns. Every field of `State` shows up in the JSON, so
/// this type IS the read model — keep it something a user can act on.
#[derive(Default, Debug, Serialize, Deserialize)]
struct Candle {
    high: f64,
    volume: f64,
    trades: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Traded {
    price: f64,
    qty: f64,
}
impl Change for Traded {}

#[derive(Default)]
struct CandleTasklet;

impl Tasklet for CandleTasklet {
    type Input = Trade;
    type Output = ();
    type State = Candle;
    type Change = Traded;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        // The middle path segment of `flz state get <operator>/<tasklet>/<key>`.
        "candle"
    }

    fn init_state(_key: Option<&str>, _p: Option<RestoreInitPayload<'_>>) -> Candle {
        Candle::default()
    }

    fn apply_change(state: &mut Candle, change: &Traded) {
        state.high = state.high.max(change.price);
        state.volume += change.qty;
        state.trades += 1;
    }

    /// A quiet symbol is evicted after ten idle minutes. Eviction is
    /// destructive, so a later read of that key is refused with
    /// `cell_evicted` rather than served a stale value.
    fn cell_idle_ttl() -> Option<Duration> {
        Some(Duration::from_secs(600))
    }

    fn run(
        &mut self,
        trade: Trade,
        mut state: StateHandle<'_, Self>,
        _ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        // Applied here, visible to a read immediately — before the flusher has
        // shipped it, and before any sink has seen it.
        state.apply(Traded {
            price: trade.price,
            qty: trade.qty,
        });
        Ok(())
    }
}

struct Market;

impl UnboundedOperator for Market {
    fn factory_id() -> &'static str {
        // The FIRST path segment of `flz state get <operator>/...`.
        "market"
    }

    fn channels(c: &ChannelBuilder) {
        // `.key_by` is what makes the Cell keys addressable by name: one
        // Cell per symbol, and `BTCUSD` is exactly what you type.
        c.input::<Trade>("trades")
            .key_by(|t: &Trade| t.symbol.clone())
            .into::<CandleTasklet>();
    }
}

fn main() {
    // Declaration-only example: `cargo run --example live_state_read` builds
    // the topology and prints the read paths it makes addressable, rather than
    // dialing a controller.
    match flezha_runner::operator::builder::build_topology_for::<Market>() {
        Ok(topology) => {
            println!("operator `{}`", topology.factory_id);
            for tasklet in &topology.tasklets {
                println!(
                    "  flz state get {}/{}/<key>   ({} mode)",
                    topology.factory_id,
                    tasklet.name,
                    if tasklet.cell_keys.is_empty() {
                        "single or dynamic"
                    } else {
                        "static"
                    }
                );
            }
        }
        Err(e) => eprintln!("topology refused: {e}"),
    }
}
