//! `ctx.part` where there is no work item to preview. The live paths run
//! against a real stream in `tests/parts.rs` and the bounded driver.

use super::spawn::NullSink;
use super::ProbeCell;
use crate::tasklet::ctx::PartRoute;
use crate::ErrorModel;

#[test]
fn a_context_with_no_work_item_refuses_a_part_as_outside_any_invocation() {
    let mut cell = ProbeCell::default();
    let mut sink = NullSink;
    let ctx = cell.ctx(&mut sink, 0, 0, b"part/test/");
    let refusal = ctx.part("preview".to_string()).expect_err("no work item");
    assert_eq!(refusal.code(), "part.outside_invocation");
    assert_eq!(refusal.to_error().details["tasklet"], "ctx_probe");
}

#[test]
fn the_testing_harness_sends_a_part_nowhere() {
    let mut cell = ProbeCell::default();
    let mut sink = NullSink;
    let ctx = cell
        .ctx(&mut sink, 0, 0, b"part/test/")
        .with_part(PartRoute::Harness);
    assert!(ctx.part("preview".to_string()).is_ok());
}
