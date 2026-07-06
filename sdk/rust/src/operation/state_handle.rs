//! `StateHandle<'_, S>` mutation chokepoint per `design.md` Decision 1.
//!
//! The dispatcher constructs a `StateHandle` borrowing the segment's
//! `&mut S::State` and `&mut Vec<S::Change>`; the user's `Operation::run`
//! receives the handle in place of `&mut S::State` and reads via the
//! `Deref` impl, mutates via `apply(change)`. The lack of a `DerefMut`
//! impl is the structural enforcement of the "no forgotten Change"
//! invariant: direct field mutation is impossible without going through
//! `apply` because the user never holds a `&mut Self::State`.

use std::cell::RefCell;
use std::ops::Deref;

use crate::operation::Operation;

/// Mutation chokepoint passed into `Operation::run` in place of
/// `&mut Self::State`. Reads via `Deref<Target = S::State>`; mutates only
/// via [`StateHandle::apply`].
///
/// The flush buffer is shared via `&RefCell` rather than `&mut` so that
/// the emit sink can drain just-applied changes onto the durable WAL path
/// *before* the matching output frame is written (flush-before-emit). Both
/// `apply` and that drain run on the same single dispatch thread inside one
/// `Operation::run`, so the `RefCell` borrows never overlap.
pub struct StateHandle<'a, S: Operation> {
    state: &'a mut S::State,
    buffer: &'a RefCell<Vec<S::Change>>,
}

impl<'a, S: Operation> StateHandle<'a, S> {
    /// Construct a handle from the segment's borrowed state + buffer.
    /// Internal: only the dispatcher and the test harness should
    /// instantiate this directly.
    #[doc(hidden)]
    pub fn new(state: &'a mut S::State, buffer: &'a RefCell<Vec<S::Change>>) -> Self {
        Self { state, buffer }
    }

    /// Apply a Change. Runs `S::apply_change(state, &change)` to fold
    /// the mutation into the underlying state, then pushes the Change
    /// onto the per-Segment flush buffer for the flusher to drain.
    /// The Change is retained for `OperationTestHarness::replay_equals`
    /// regardless of `UnboundedOperative::PERSISTENCE_ENABLED`.
    pub fn apply(&mut self, change: S::Change) {
        S::apply_change(self.state, &change);
        self.buffer.borrow_mut().push(change);
    }
}

impl<'a, S: Operation> Deref for StateHandle<'a, S> {
    type Target = S::State;
    fn deref(&self) -> &S::State {
        self.state
    }
}
