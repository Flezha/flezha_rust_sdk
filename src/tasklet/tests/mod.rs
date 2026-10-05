mod determinism;
mod effect;
mod one_form;
mod part;
mod spawn;
mod trait_defaults;

use std::cell::RefCell;

use crate::tasklet::ctx::EmitSink;
use crate::tasklet::{Ctx, NoChange, Tasklet, TaskletError};

/// A stateless tasklet emitting strings: what the context tests stand a
/// `Ctx` up for.
#[derive(Default)]
struct Probe;

#[flezha_runner::tasklet(name = "ctx_probe")]
impl Tasklet for Probe {
    type Input = ();
    type Output = String;

    fn run(&mut self, _: (), _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

/// The Cell a probe context borrows: no state and an empty change buffer.
#[derive(Default)]
struct ProbeCell {
    state: (),
    changes: RefCell<Vec<NoChange>>,
}

impl ProbeCell {
    fn ctx<'a>(
        &'a mut self,
        sink: &'a mut dyn EmitSink,
        now: u64,
        seq: u64,
        identity: &'a [u8],
    ) -> Ctx<'a, Probe> {
        Ctx::new(&mut self.state, &self.changes, sink, now, seq, identity)
    }
}
