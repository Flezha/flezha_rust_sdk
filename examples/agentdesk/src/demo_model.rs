//! The demo model: a canned answer, one token at a time, and the number of
//! tokens it used. It stands in for a real model so the example runs anywhere;
//! a real model's client would be called here instead, handing each token it
//! streams to `part` the same way.

use std::time::Duration;

use flezha_runner::TaskletError;

use crate::reply::Answer;

const MAX_MESSAGE: usize = 200;
const PER_TOKEN: Duration = Duration::from_millis(50);

pub fn answer(
    earlier: &[String],
    message: &str,
    part: impl Fn(Answer) -> Result<(), TaskletError>,
) -> Result<Answer, TaskletError> {
    let length = message.chars().count();
    if length > MAX_MESSAGE {
        return Err(TaskletError::NonRetryable(format!(
            "the demo model reads at most {MAX_MESSAGE} characters, and this message has {length}"
        )));
    }
    let text = format!(
        "Message {}: you said \"{message}\". I am a demo model.",
        earlier.len() + 1
    );
    let tokens: Vec<&str> = text.split_inclusive(' ').collect();
    for (i, token) in tokens.iter().enumerate() {
        part(Answer {
            text: token.to_string(),
            tokens: i as u32 + 1,
        })?;
        std::thread::sleep(PER_TOKEN);
    }
    Ok(Answer {
        tokens: tokens.len() as u32,
        text,
    })
}
