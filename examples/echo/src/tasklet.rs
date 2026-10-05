use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

/// A processing stage of the relay's graph, run once per message. This one
/// prefixes the input and emits the result.
///
/// The tasklet is your own type. The attribute generates no type: it fills in
/// what the block leaves out. With no `type State` written, nothing is kept
/// between messages and nothing is journaled. `name` is the tasklet's stable
/// identity. State is filed under it, so renaming it later declares a different
/// tasklet rather than renaming this one; renaming the type is safe.
///
/// Where the result goes is not decided here. The relay decides that, so
/// the graph can be rewired without touching this tasklet.
#[derive(Default)]
pub struct Echo;

// state is saved under this name, so choose it once
#[tasklet(name = "echo")]
impl Tasklet for Echo {
    type Input = String;
    type Output = String;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("echo: {input}")) // where it goes is the relay's business
    }
}
