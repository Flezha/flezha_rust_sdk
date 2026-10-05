use flezha_runner::{ChannelBuilder, Each, OnFailure, Relay, RelaySettings, Run};
use serde::{Deserialize, Serialize};

use crate::confirm_fill::ConfirmFill;
use crate::place_order::PlaceOrder;
use crate::record_order::RecordOrder;
use crate::reserve_funds::ReserveFunds;

/// The order an instance is started with. The entry channel declares this
/// type, so the first message is decoded into it:
/// `--text '{"symbol": "EUR/USD", "quantity": 10}' --channel order`.
#[derive(Clone, Serialize, Deserialize)]
pub struct OrderTicket {
    pub symbol: String,
    pub quantity: u32,
}

/// The desk's steps relay: one instance per order, which runs its steps
/// once and then ends.
///
/// A steps relay declares its steps the way a flow relay declares
/// its graph, on the same builder, and the controller schedules, restarts and
/// relocates it in the same way. What differs is the lifecycle: each step's
/// output is written down as it commits, so an instance that is interrupted
/// resumes at the step it reached rather than from the beginning, and the
/// instance ends by itself once the chain has run.
///
/// When a step fails and its retries are spent, the instance ends abnormally
/// instead, and the default rollback undoes every step that finished, in
/// reverse, through their `compensate` hooks: the order is cancelled, then the
/// money is released. A step with no `compensate` is skipped, which is why the
/// desk's book is left alone.
pub struct OrderExecutorRelay;

impl Relay for OrderExecutorRelay {
    /// The relay's stable identity, and the name
    /// `flz relay start order_executor` resolves. It is durable, so changing
    /// it declares a new relay rather than renaming this one.
    fn factory_id() -> &'static str {
        "order_executor"
    }

    /// Two steps in order, then a fan-out. Confirming the fill and booking the
    /// order are two independent consequences of the placement, and neither
    /// waits for the other. Each arm of `branch` receives the same handle.
    ///
    /// The driver runs the chain level by level: every step of a level commits
    /// before any of its children run, so the desk's order of events is fixed
    /// rather than a race. A step that fails poisons its whole level, which is
    /// why a refusal on either arm unwinds what the other arm did.
    ///
    /// Nothing ends on an output: the last step ends the instance with its result,
    /// which `flz relay info` shows.
    fn channels(c: &ChannelBuilder) {
        c.input::<OrderTicket>("order") // the order to carry out
            .into::<ReserveFunds>() // step one
            .into::<PlaceOrder>() // step two, once step one is done
            .branch([
                &|fill| {
                    fill.into::<ConfirmFill>(); // two arms, side by side
                },
                &|book| {
                    book.into::<RecordOrder>();
                },
            ]);
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            // one instance per order; each step runs once
            // a failed step undoes the finished ones
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            ..RelaySettings::default()
        }
    }
}
