use flezha_runner::{ChannelBuilder, StreamOperative};

use crate::procedure::EchoProcedure;

/// The deployable unit. An operative declares the graph a message travels: the
/// entry channel it arrives on, the steps it passes through, and the channel its
/// result leaves on.
///
/// The operative is what the controller schedules onto a runner, restarts after
/// a failure and moves when a runner goes away. A procedure is one stage inside
/// that graph.
pub struct EchoOperative;

impl StreamOperative for EchoOperative {
    /// The operative's stable identity, and the name `flz operative start`
    /// takes. The controller's registry and the durable state both key on it,
    /// so changing it declares a new operative rather than renaming this one.
    fn factory_id() -> &'static str {
        "echo"
    }

    /// Called once when the runner declares the operative. The graph is checked
    /// as a whole there, so one that cannot work is refused at startup.
    ///
    /// An entry channel named `in` carrying strings, routed into the echo
    /// procedure, whose output ends on the plugin layer as the channel `out`.
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<EchoProcedure>()
            .sink_named("out");
    }
}
