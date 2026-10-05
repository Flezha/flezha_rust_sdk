use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

use crate::http::Http;
use crate::request::Request;

/// Answers a request for a session's events with its stream, and any other request with 404.
#[derive(Default)]
pub struct Watch;

#[tasklet(name = "watch")]
impl Tasklet for Watch {
    type Input = Request;
    type Output = Http;

    fn run(&mut self, request: Request, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        // keyed by the user: an output of an effectively-once relay needs a key
        ctx.emit_keyed(
            request.user(),
            if request.listens() {
                Http::Stream {
                    stream: request.user().to_string(),
                }
            } else {
                Http::Plain {
                    status: 404,
                    body: "no such route".to_string(),
                }
            },
        )
    }
}
