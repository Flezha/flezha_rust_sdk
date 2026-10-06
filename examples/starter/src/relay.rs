use flezha_runner::{ChannelBuilder, Relay};
use serde_json::json;

use crate::count::{CountHits, Hit};

/// The relay: a visit arrives on `hit`, goes to the cell of its page, and the
/// count leaves on the output `counts`, which also answers a caller.
pub struct StarterRelay;

impl Relay for StarterRelay {
    fn factory_id() -> &'static str {
        "starter" // the name `flz relay start` takes
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<Hit>("hit")
            .describe("Counts visits per page") // shown by `flz relay info` and to AI agents
            .schema(json!({
                "type": "object",
                "properties": {"page": {"type": "string"}},
                "required": ["page"]
            }))
            .key_by(|hit| hit.page.clone()) // one cell, and one count, per page
            .into::<CountHits>()
            .output_named("counts");
    }
}
