use std::sync::Arc;

use flezha_runner::{tasklet, Change, Ctx, Tasklet, TaskletError};
use serde::{Deserialize, Serialize};

use crate::order_executor::OrderTicket;

/// What it hands on.
#[derive(Clone, Serialize, Deserialize)]
pub struct Reserved {
    pub order: OrderTicket,
    pub reservation_id: String,
}

/// What this step remembers.
#[derive(Default, Serialize, Deserialize)]
pub struct Reservation {
    id: String,
}

/// How the id is written down.
#[derive(Serialize, Deserialize)]
pub struct Reserving(String);

impl Change<Reservation> for Reserving {
    fn apply_to(&self, reservation: &mut Reservation) {
        reservation.id = self.0.clone();
    }
}

/// Step one: put the money for the order aside, and release it again if the
/// order is undone.
///
/// The reservation id comes from `ctx.stable_id()`, and the step both
/// remembers it and hands it on.
#[derive(Default)]
pub struct ReserveFunds;

#[tasklet(name = "reserve_funds")]
impl Tasklet for ReserveFunds {
    type Input = OrderTicket;
    type Output = Reserved;
    type State = Reservation;
    type Change = Reserving;

    // The step: reserve the money under an id, remember the id, and hand the order on.
    fn run(&mut self, order: OrderTicket, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let id = format!("res-{}", ctx.stable_id()); // the same id when the step runs again
        ctx.change(Reserving(id.clone()));
        ctx.emit(Reserved {
            order,
            reservation_id: id,
        })
    }

    // The undo: release the money, by the id the state kept.
    fn compensate(
        &mut self,
        _: Vec<Arc<Reserved>>,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        println!("[undo] released {}", ctx.state().id);
        Ok(())
    }
}
