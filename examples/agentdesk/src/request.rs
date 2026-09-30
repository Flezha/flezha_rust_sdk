use serde::Deserialize;

use crate::agent::Message;

/// One HTTP request, as the plugin hands it to the gateway. The gateway knows
/// two routes: `POST /sessions/<user>/messages`, whose body is the message, and
/// `GET /sessions/<user>/events`, the session's answers as they are written.
/// Fields the envelope carries beside these are ignored.
#[derive(Clone, Deserialize)]
pub struct Request {
    method: String,
    path: String,
    body: Option<String>,
}

impl Request {
    pub fn sends(&self) -> bool {
        self.is("POST", "messages")
    }

    pub fn listens(&self) -> bool {
        self.is("GET", "events")
    }

    pub fn user(&self) -> &str {
        self.route().map_or("", |(user, _)| user)
    }

    pub fn message(&self) -> Message {
        Message {
            user: self.user().to_string(),
            text: self.body.clone().unwrap_or_default(),
        }
    }

    fn is(&self, verb: &str, what: &str) -> bool {
        self.method == verb && self.route().is_some_and(|(_, route)| route == what)
    }

    // `/sessions/<user>/<what>`, the user a short lower-case name; `None` when not a route.
    fn route(&self) -> Option<(&str, &str)> {
        let (user, what) = self.path.strip_prefix("/sessions/")?.split_once('/')?;
        let name = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-';
        let is_user = user.len() <= 64
            && user.starts_with(|c: char| c.is_ascii_lowercase())
            && user.chars().all(name);
        (is_user && matches!(what, "messages" | "events")).then_some((user, what))
    }
}
