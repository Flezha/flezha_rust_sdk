use std::sync::Arc;

use flezha_runner::{tasklet, Change, Ctx, Tasklet, TaskletError};
use serde::{Deserialize, Serialize};

use crate::order_executor::OrderTicket;
use crate::reserve_funds::Reserved;

/// What it hands on.
#[derive(Clone, Serialize, Deserialize)]
pub struct PlacedOrder {
    pub order: OrderTicket,
    pub order_id: String,
}

/// What this step remembers.
#[derive(Default, Serialize, Deserialize)]
pub struct Placement {
    order_id: String,
}

/// How the id is written down.
#[derive(Serialize, Deserialize)]
pub struct Placing(String);

impl Change<Placement> for Placing {
    fn apply_to(&self, placement: &mut Placement) {
        placement.order_id = self.0.clone();
    }
}

/// Step two: place the order with the paper exchange, keeping the id it
/// answers with, and cancel the order again if it is undone. It has the same
/// shape as step one.
#[derive(Default)]
pub struct PlaceOrder;

#[tasklet(name = "place_order")]
impl Tasklet for PlaceOrder {
    type Input = Reserved;
    type Output = PlacedOrder;
    type State = Placement;
    type Change = Placing;

    // The step: place the order under an id, remember the id, and hand the order on.
    fn run(&mut self, reserved: Reserved, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let order_id = format!("ord-{}", ctx.stable_id());
        ctx.change(Placing(order_id.clone()));
        ctx.emit(PlacedOrder {
            order: reserved.order,
            order_id,
        })
    }

    // The undo: cancel the order, by the id the state kept.
    fn compensate(
        &mut self,
        _: Vec<Arc<PlacedOrder>>,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        println!("[undo] cancelled {}", ctx.state().order_id);
        Ok(())
    }
}
