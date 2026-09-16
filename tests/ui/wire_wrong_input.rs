// `stream.into::<C>()` where the stream's payload is TradeMsg but
// C::Input = Candle. Should fail with a clear "expected TradeMsg, found
// Candle" trait-bound error pointing at the `.into::<ConsumesCandle>()`
// call — the channel payload type comes from the producer's `Output`.

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct TradeMsg;
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Candle;

#[derive(Default)]
struct ProducesTrade;
impl Procedure for ProducesTrade {
    type Input = String;
    type Output = TradeMsg;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str { "produces_trade" }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(&mut self, _: String, _: StateHandle<'_, Self>, _: &mut ProcedureCtx<'_>) -> Result<(), ProcedureError> {
        Ok(())
    }
}

#[derive(Default)]
struct ConsumesCandle;
impl Procedure for ConsumesCandle {
    type Input = Candle;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str { "consumes_candle" }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(&mut self, _: Candle, _: StateHandle<'_, Self>, _: &mut ProcedureCtx<'_>) -> Result<(), ProcedureError> {
        Ok(())
    }
}

fn declare(c: &ChannelBuilder) {
    c.input::<String>("in")
        .into::<ProducesTrade>()
        .into::<ConsumesCandle>();
}

fn main() {}
