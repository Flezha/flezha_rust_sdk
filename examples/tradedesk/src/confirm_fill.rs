use std::time::Duration;

use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

use crate::place_order::PlacedOrder;

const EXCHANGE_MAX: u32 = 100;

/// Asks the exchange whether the order filled, on one arm of the fan-out. The
/// paper exchange rejects anything above its own size, which is what makes this
/// the step that fails, and takes ten seconds to answer, which leaves time to
/// stop the runner in the middle of an order.
///
/// It remembers nothing and has nothing to undo, so it writes only `run`. It
/// ends the instance with a result rather than emitting one: the order's
/// outcome is the instance's own answer, which `flz relay info` reads back,
/// so the desk needs no output here.
///
/// A failure here is retried first, and once the retries are spent the instance
/// ends abnormally. Everything that finished is then undone in reverse, across
/// both arms: the order is cancelled and the money released, while the desk's
/// book is skipped because it writes no `compensate`.
#[derive(Default)]
pub struct ConfirmFill;

#[tasklet(name = "confirm_fill")]
impl Tasklet for ConfirmFill {
    type Input = PlacedOrder;

    fn run(&mut self, placed: PlacedOrder, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        std::thread::sleep(Duration::from_secs(10)); // the paper exchange takes ten seconds to answer
        let quantity = placed.order.quantity;
        if quantity > EXCHANGE_MAX {
            return Err(TaskletError::Custom(format!(
                "the exchange fills at most {EXCHANGE_MAX} a time, and this is {quantity}"
            ))); // tried again, then the order is undone
        }
        ctx.complete(format!(
            "filled {} {quantity} as {}",
            placed.order.symbol, placed.order_id
        ))
    }
}
