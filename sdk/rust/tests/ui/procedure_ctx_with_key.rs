// The body of a context-parameter `run` keys its own emits, so `key` has
// nothing left to name — a silently ignored attribute would be the worst
// outcome for a keyed sink.
fn sk(_out: &u64) -> String {
    String::new()
}

#[flezha_runner::procedure(id = "p", key = sk, emits = u64)]
fn p(_input: u64, ctx: &mut flezha_runner::ProcedureCtx) -> Result<(), flezha_runner::ProcedureError> {
    ctx.emit::<u64>(1)
}

fn main() {}
