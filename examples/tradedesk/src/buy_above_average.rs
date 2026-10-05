use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

use crate::order_executor::OrderTicket;
use crate::track_prices::Move;

const BUY_ABOVE_PCT: f64 = 2.0;
const UNITS_PER_PERCENT: f64 = 10.0;

/// Buys when a price is far enough above its symbol's average.
///
/// It remembers nothing: the move it is handed already carries the average, so
/// the decision is a function of one value, and a restart costs this stage
/// nothing because there is nothing to bring back. That is why it writes no
/// `type State`, beside `TrackPrices`, which keeps the window.
///
/// It does two things per input, both through the context: it starts an
/// relay that runs in steps and it emits.
#[derive(Default)]
pub struct BuyAboveAverage;

#[tasklet(name = "buy_above_average")]
impl Tasklet for BuyAboveAverage {
    type Input = Move;
    type Output = String;

    fn run(&mut self, m: Move, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let percent = (m.price - m.average) / m.average * 100.0;
        if percent < BUY_ABOVE_PCT {
            return Ok(());
        }
        let quantity = (percent * UNITS_PER_PERCENT).round() as u32; // a stronger move buys more

        let ticket = OrderTicket {
            symbol: m.symbol.clone(),
            quantity,
        };
        let order = serde_json::to_vec(&ticket).map_err(|e| TaskletError::Custom(e.to_string()))?;
        let _ = ctx.spawn_steps("order_executor", order); // one steps relay per order

        ctx.emit(format!(
            "{} {:.4} is {:.2}% above its average of {:.4}, buying {}",
            m.symbol, m.price, percent, m.average, quantity
        ))
    }
}
