use flezha_runner::{ChannelBuilder, Relay};

use crate::tasklet::Echo;

/// The deployable unit. A relay declares the graph a message travels: the
/// entry channel it arrives on, the steps it passes through, and the channel its
/// result leaves on.
///
/// The relay is what the controller schedules onto a runner, restarts after
/// a failure and moves when a runner goes away. A tasklet is one stage inside
/// that graph.
pub struct EchoRelay;

impl Relay for EchoRelay {
    /// The relay's stable identity, and the name `flz relay start`
    /// takes. The controller's registry and the durable state both key on it,
    /// so changing it declares a new relay rather than renaming this one.
    fn factory_id() -> &'static str {
        "echo" // the name `flz relay start` takes
    }

    /// Called once when the runner declares the relay. The graph is checked
    /// as a whole there, so one that cannot work is refused at startup.
    ///
    /// An entry channel named `in` carrying strings, routed into the echo
    /// tasklet, whose output ends on the plugin layer as the channel `out`.
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in") // messages arrive on the entry channel "in"
            .into::<Echo>() // each one runs through the echo tasklet
            .output_named("out"); // the result leaves on "out", for a plugin to pick up
    }
}
