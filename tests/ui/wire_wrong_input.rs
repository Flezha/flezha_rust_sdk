// `stream.into::<C>()` where the stream's payload is TradeMsg but
// C::Input = Candle. Should fail with a clear "expected TradeMsg, found
// Candle" trait-bound error pointing at the `.into::<ConsumesCandle>()`
// call — the channel payload type comes from the producer's `Output`.

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct TradeMsg;
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Candle;

#[derive(Default)]
struct ProducesTrade;
#[tasklet(name = "produces_trade")]
impl Tasklet for ProducesTrade {
    type Input = String;
    type Output = TradeMsg;
    fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

#[derive(Default)]
struct ConsumesCandle;
#[tasklet(name = "consumes_candle")]
impl Tasklet for ConsumesCandle {
    type Input = Candle;
    fn run(&mut self, _: Candle, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

fn declare(c: &ChannelBuilder) {
    c.input::<String>("in")
        .into::<ProducesTrade>()
        .into::<ConsumesCandle>();
}

fn main() {}
