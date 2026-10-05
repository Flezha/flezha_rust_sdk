use std::collections::VecDeque;

use flezha_runner::{tasklet, Change, Ctx, Tasklet, TaskletError};
use serde::{Deserialize, Serialize};

/// One trade. Fields the exchange sends beside these are ignored.
#[derive(Clone, Deserialize)]
pub struct Trade {
    pub symbol: String,
    pub price: f64,
    pub ord_type: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Move {
    pub symbol: String,
    pub price: f64,
    pub average: f64,
}

const WINDOW: usize = 10;

/// What each symbol's cell remembers.
#[derive(Default, Serialize, Deserialize)]
pub struct PriceWindow {
    prices: VecDeque<f64>,
}

/// How a price joins the window.
#[derive(Serialize, Deserialize)]
pub struct PriceSeen(f64);

impl Change<PriceWindow> for PriceSeen {
    fn apply_to(&self, window: &mut PriceWindow) {
        window.prices.push_back(self.0);
        if window.prices.len() > WINDOW {
            window.prices.pop_front();
        }
    }
}

/// Keeps a symbol's last ten prices, and compares each new trade with their
/// average.
///
/// One cell per symbol, because the relay keys the channel by symbol, and
/// the window is that cell's state. Every change to it is recorded as it
/// happens, so a runner that is killed and started again finds the window it
/// had rather than an empty one.
#[derive(Default)]
pub struct TrackPrices;

#[tasklet(name = "track_prices")]
impl Tasklet for TrackPrices {
    type Input = Trade;
    type Output = Move;
    type State = PriceWindow;
    type Change = PriceSeen;

    fn run(&mut self, trade: Trade, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let prices = &ctx.state().prices;
        if !prices.is_empty() {
            // a symbol's first trade has nothing to compare with
            let average = prices.iter().sum::<f64>() / prices.len() as f64;
            ctx.emit(Move {
                symbol: trade.symbol,
                price: trade.price,
                average,
            })?;
        }
        ctx.change(PriceSeen(trade.price));
        Ok(())
    }
}
