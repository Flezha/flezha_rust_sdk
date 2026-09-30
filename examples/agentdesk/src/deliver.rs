use std::sync::Arc;

use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

use crate::reply::Answer;

/// Hands the finished answer to the caller: a channel has one end, and `answer` is a sink.
#[derive(Default)]
pub struct Deliver;

#[tasklet(name = "deliver")]
impl Tasklet for Deliver {
    type Input = Answer;
    type Output = Answer;

    fn run(&mut self, answer: Answer, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(answer)
    }

    // The undo, when a turn is taken back: an answer already given has nothing to take back.
    fn compensate(
        &mut self,
        _: Vec<Arc<Answer>>,
        _: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        Ok(())
    }
}
