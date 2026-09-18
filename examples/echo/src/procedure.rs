use flezha_runner::procedure;

/// A processing stage of the operative's graph, run once per message. This one
/// prefixes the input, and what it returns is emitted.
///
/// The attribute generates the procedure type `Echo` from the function: the
/// input and output types are read from its signature, and with no `&mut`
/// state parameter nothing is kept between messages and nothing is journaled.
/// `id` is the procedure's stable identity. State is filed under it, so
/// renaming it later declares a different procedure rather than renaming this
/// one.
///
/// Where the result goes is not decided here. The operative decides that, so
/// the graph can be rewired without touching this procedure.
#[procedure(id = "echo")]
pub fn echo(input: String) -> String {
    format!("echo: {input}")
}
