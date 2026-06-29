// `c.channel::<TradeMsg>("...").from::<S>().into::<C>()` where C::Input =
// Candle. Should fail with a clear "expected TradeMsg, found Candle"
// trait-bound error pointing at the `.into::<ConsumesCandle>()` call.

use flezha_executor::channel::ChannelBuilder;
use flezha_executor::operation::{Operation, OperationCtx, OperationError};
use flezha_executor::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};

#[derive(Clone)]
struct TradeMsg;
#[derive(Clone)]
struct Candle;

#[derive(Default)]
struct ProducesTrade;
impl Operation for ProducesTrade {
    type Input = ();
    type Output = TradeMsg;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str { "produces_trade" }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(&mut self, _: (), _: StateHandle<'_, Self>, _: &mut OperationCtx<'_>) -> Result<(), OperationError> {
        Ok(())
    }
}

#[derive(Default)]
struct ConsumesCandle;
impl Operation for ConsumesCandle {
    type Input = Candle;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str { "consumes_candle" }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(&mut self, _: Candle, _: StateHandle<'_, Self>, _: &mut OperationCtx<'_>) -> Result<(), OperationError> {
        Ok(())
    }
}

fn declare(c: &mut ChannelBuilder) {
    c.channel::<TradeMsg>("trade.parsed")
        .from::<ProducesTrade>()
        .into::<ConsumesCandle>();
}

fn main() {}
