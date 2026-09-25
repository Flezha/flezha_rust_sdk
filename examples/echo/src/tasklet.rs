use flezha_runner::tasklet;

/// A processing stage of the operator's graph, run once per message. This one
/// prefixes the input, and what it returns is emitted.
///
/// The attribute generates the tasklet type `Echo` from the function: the
/// input and output types are read from its signature, and with no `&mut`
/// state parameter nothing is kept between messages and nothing is journaled.
/// `id` is the tasklet's stable identity. State is filed under it, so
/// renaming it later declares a different tasklet rather than renaming this
/// one.
///
/// Where the result goes is not decided here. The operator decides that, so
/// the graph can be rewired without touching this tasklet.
#[tasklet(id = "echo")]
pub fn echo(input: String) -> String {
    format!("echo: {input}")
}
