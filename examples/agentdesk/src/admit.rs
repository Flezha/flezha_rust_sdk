use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

use crate::budget::{Verdict, DAILY_TOKENS};
use crate::http::Http;

/// Answers a message 202 when it goes on to the session, and 429 when its user is over the budget.
#[derive(Default)]
pub struct Admit;

#[tasklet(name = "admit")]
impl Tasklet for Admit {
    type Input = Verdict;
    type Output = Http;

    fn run(&mut self, verdict: Verdict, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let user = &verdict.message.user;
        ctx.emit_keyed(
            user.as_str(),
            if verdict.admitted() {
                Http::Plain {
                    status: 202,
                    body: format!("sent to {user}"),
                }
            } else {
                Http::Plain {
                    status: 429,
                    body: format!(
                        "{user} has used {} of today's {DAILY_TOKENS} tokens",
                        verdict.spent
                    ),
                }
            },
        )
    }
}
