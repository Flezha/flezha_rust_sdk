use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

use crate::place_order::PlacedOrder;

/// Writes the placed order into the desk's book, on its own arm of the fan-out.
///
/// Confirming the fill and booking the order are two independent consequences
/// of the same event, and neither waits for the other, which is what a branch
/// is for.
///
/// The plainest shape a tasklet has: a value in, nothing kept, nothing out.
/// With no `type Output` written it declares no output channel at all.
///
/// It writes no `compensate` on purpose. A rollback undoes what the desk did to
/// the world, not what it wrote down about it: the attempt happened, and the
/// book says so even when the order was cancelled again.
#[derive(Default)]
pub struct RecordOrder;

#[tasklet(name = "record_order")]
impl Tasklet for RecordOrder {
    type Input = PlacedOrder;

    fn run(&mut self, placed: PlacedOrder, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        println!(
            "[book] {} {} as {}",
            placed.order.symbol, placed.order.quantity, placed.order_id
        );
        Ok(())
    }
}
