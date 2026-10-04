use flezha_runner::{tasklet, Change, Ctx, Response, Tasklet, TaskletError};
use serde::{Deserialize, Serialize};

use crate::agent::Message;
use crate::reply::Answer;

pub const DAILY_TOKENS: u32 = 40;
const DAY_MS: u64 = 86_400_000;

/// What reaches a user's cell: a message to admit, or the tokens an answer used.
#[derive(Clone)]
pub enum Event {
    Sent(Message),
    Used { user: String, tokens: u32 },
}

impl Event {
    pub fn user(&self) -> &str {
        match self {
            Event::Sent(message) => &message.user,
            Event::Used { user, .. } => user,
        }
    }

    // A failed turn used no tokens.
    pub fn used(response: Response<Message, Answer>) -> Self {
        Event::Used {
            user: response.request.user,
            tokens: response.result.map_or(0, |answer| answer.tokens),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Verdict {
    pub message: Message,
    pub spent: u32,
}

impl Verdict {
    pub fn admitted(&self) -> bool {
        self.spent < DAILY_TOKENS
    }
}

/// What each user's cell remembers: the day, counted in days since 1970 in UTC, and its tokens.
#[derive(Default, Serialize, Deserialize)]
pub struct Spent {
    day: u64,
    tokens: u32,
}

impl Spent {
    fn on(&self, today: u64) -> u32 {
        if today == self.day {
            self.tokens
        } else {
            0
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Counted {
    day: u64,
    tokens: u32,
}

impl Change<Spent> for Counted {
    fn apply_to(&self, spent: &mut Spent) {
        spent.tokens = spent.on(self.day) + self.tokens;
        spent.day = self.day;
    }
}

/// Keeps the tokens a user's answers used today, and admits a message while
/// they are under the day's budget. One cell per user.
#[derive(Default)]
pub struct Budget;

#[tasklet(name = "budget")]
impl Tasklet for Budget {
    type Input = Event;
    type Output = Verdict;
    type State = Spent;
    type Change = Counted;

    fn run(&mut self, event: Event, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let today = ctx.now() / DAY_MS;
        match event {
            // A message to admit: hand on its verdict.
            Event::Sent(message) => ctx.emit(Verdict {
                message,
                spent: ctx.state().on(today),
            }),
            // The tokens an answer used: count them.
            Event::Used { tokens, .. } => {
                ctx.change(Counted { day: today, tokens });
                Ok(())
            }
        }
    }
}
