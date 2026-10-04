use std::sync::Arc;

use flezha_runner::{tasklet, Change, Ctx, Tasklet, TaskletError};
use serde::{Deserialize, Serialize};

use crate::demo_model;

#[derive(Clone, Serialize, Deserialize)]
pub struct Answer {
    pub text: String,
    pub tokens: u32,
}

/// What the session remembers.
#[derive(Default, Serialize, Deserialize)]
pub struct Conversation {
    messages: Vec<String>,
}

/// How the conversation changes: a message said, or the last one taken back.
#[derive(Serialize, Deserialize)]
pub enum Edit {
    Said(String),
    Unsaid,
}

impl Change<Conversation> for Edit {
    fn apply_to(&self, conversation: &mut Conversation) {
        match self {
            Edit::Said(message) => conversation.messages.push(message.clone()),
            Edit::Unsaid => {
                conversation.messages.pop();
            }
        }
    }
}

/// Answers a message with the demo model, sending the answer as it grows, and
/// keeps the conversation so the next answer knows what came before.
#[derive(Default)]
pub struct Reply;

#[tasklet(name = "reply")]
impl Tasklet for Reply {
    type Input = String;
    type Output = Answer;
    type State = Conversation;
    type Change = Edit;

    fn run(&mut self, message: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        // Each piece streams out as it is made; then the whole answer.
        ctx.emit(demo_model::answer(
            &ctx.state().messages,
            &message,
            |part| ctx.part(part),
        )?)?;
        ctx.change(Edit::Said(message));
        Ok(())
    }

    // The undo, when a turn is taken back: the conversation forgets its message.
    fn compensate(
        &mut self,
        _: Vec<Arc<Answer>>,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        println!(
            "[undo] forgot \"{}\"",
            ctx.state().messages.last().map_or("", String::as_str)
        );
        ctx.change(Edit::Unsaid);
        Ok(())
    }
}
