//! A sugared tasklet's context parameter keeps the type its author wrote, so
//! an imported `TaskletCtx` is used by the expansion and never reported as an
//! unused import.
#![deny(unused_imports)]

use flezha_runner::tasklet::{TaskletCtx, TaskletError};
use flezha_runner::testing::TaskletTestHarness;

#[flezha_runner::tasklet(id = "split_words", emits = String)]
fn split_words(line: String, ctx: &mut TaskletCtx<'_>) -> Result<(), TaskletError> {
    for word in line.split_whitespace() {
        ctx.emit(word.to_string())?;
    }
    Ok(())
}

/// fails when: the expansion replaces the written context type again — this
/// file then stops compiling on its own `deny(unused_imports)`.
#[test]
fn an_imported_ctx_type_is_the_one_the_expansion_uses() {
    let mut harness = TaskletTestHarness::<SplitWords>::new();
    harness.apply_inputs(vec!["a b".to_string()]);
    harness.expect_emits(&["a".to_string(), "b".to_string()]);
}
