// `c.channel::<TradeMsg>("...").from::<S>()` where S::Output = Candle.
// Should fail with a clear "expected TradeMsg, found Candle" trait-bound
// error pointing at the `.from::<ProducesCandle>()` call.

use flezha_executor::channel::ChannelBuilder;
use flezha_executor::operation::{Operation, OperationCtx, OperationError};
use flezha_executor::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};

struct TradeMsg;
struct Candle;

#[derive(Default)]
struct ProducesCandle;
impl Operation for ProducesCandle {
    type Input = ();
    type Output = Candle;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str { "produces_candle" }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(&mut self, _: (), _: StateHandle<'_, Self>, _: &mut OperationCtx<'_>) -> Result<(), OperationError> {
        Ok(())
    }
}

fn declare(c: &mut ChannelBuilder) {
    c.channel::<TradeMsg>("trade.parsed")
        .from::<ProducesCandle>();
}

fn main() {}
