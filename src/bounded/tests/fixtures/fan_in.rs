//! Fan-in and diamond tasklets: the join that accumulates both branch
//! outputs, and the merge and mapping shapes around it.

use std::sync::{Arc, Mutex};

use super::*;
use crate::bounded::*;
use crate::cell::restore::RestoreInitPayload;
use crate::channel::ChannelBuilder;
use crate::tasklet::change::NoChange;
use crate::tasklet::clock::WallClock;
use crate::tasklet::{StateHandle, Tasklet, TaskletCtx, TaskletError};

// ---- fan-in / diamond tasks (`bounded-branching` fan-in accumulates) ------

/// Observation log for the diamond scenarios: forward runs append here so
/// join dispatch order (after BOTH producers) is assertable.
pub(crate) static DIAMOND_LOG: Mutex<Vec<String>> = Mutex::new(Vec::new());
pub(crate) fn diamond_log(entry: String) {
    DIAMOND_LOG.lock().unwrap().push(entry);
}
pub(crate) fn drain_diamond_log() -> Vec<String> {
    std::mem::take(&mut *DIAMOND_LOG.lock().unwrap())
}

macro_rules! diamond_op {
    ($ty:ident, $name:literal, $tag:literal) => {
        #[derive(Default)]
        struct $ty;
        impl Tasklet for $ty {
            type Input = String;
            type Output = String;
            type State = ();
            type Change = NoChange;
            type Clock = WallClock;
            type Config = ();
            fn name() -> &'static str {
                $name
            }
            fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
            fn run(
                &mut self,
                input: String,
                _state: StateHandle<'_, Self>,
                ctx: &mut TaskletCtx<'_>,
            ) -> Result<(), TaskletError> {
                diamond_log(format!("{}:{input}", $tag));
                ctx.emit(format!("{}:{input}", $tag))?;
                Ok(())
            }
            const COMPENSATE_OVERRIDDEN: bool = true;
            fn compensate(
                &mut self,
                completed_output: Vec<Arc<String>>,
                _state: StateHandle<'_, Self>,
                _ctx: &mut TaskletCtx<'_>,
            ) -> Result<(), TaskletError> {
                for out in &completed_output {
                    comp_log(format!("undo-{}:{out}", $tag));
                }
                Ok(())
            }
        }
    };
}

diamond_op!(DiamondA, "diamond_a", "a");
diamond_op!(DiamondB, "diamond_b", "b");
diamond_op!(DiamondC, "diamond_c", "c");
diamond_op!(DiamondJoin, "bounded_join_op", "j");

/// The diamond A→(B,C)→D: both branch outputs accumulate into the one
/// join node (two unnamed mentions unify), which closes plugin-bound.
pub(crate) struct DiamondBounded;
impl BoundedOperator for DiamondBounded {
    fn factory_id() -> &'static str {
        "test-diamond-bounded"
    }
    fn channels(c: &ChannelBuilder) {
        let a = c.input::<String>("in").into::<DiamondA>().named("d_fan");
        a.into::<DiamondB>().named("d_b").into::<DiamondJoin>();
        a.into::<DiamondC>()
            .named("d_c")
            .into::<DiamondJoin>()
            .sink_named("d_out");
    }
}

/// Kahn-shape twin used by the registration test (no driver run).
pub(crate) struct JoinBounded;
impl BoundedOperator for JoinBounded {
    fn factory_id() -> &'static str {
        "test-join-bounded-shape"
    }
    fn channels(c: &ChannelBuilder) {
        let mid = c.input::<String>("in").into::<Stage1>().named("mid");
        mid.into::<Stage2>().named("join_b").into::<DiamondJoin>();
        mid.into::<EchoTask>().named("join_c").into::<DiamondJoin>();
    }
}

/// Diamond whose join feeds a failing tail: A→(B,C)→D→ship(fails). The
/// abort must undo the join BEFORE either producer, and the shared root
/// last (reverse-topological compensation over a DAG).
pub(crate) struct DiamondSaga;
impl BoundedOperator for DiamondSaga {
    fn factory_id() -> &'static str {
        "test-diamond-saga"
    }
    fn channels(c: &ChannelBuilder) {
        let a = c.input::<String>("in").into::<DiamondA>().named("d_fan");
        let join = a
            .into::<DiamondB>()
            .named("d_b")
            .into::<DiamondJoin>()
            .named("d_join");
        a.into::<DiamondC>().named("d_c").into::<DiamondJoin>();
        join.into::<SagaShipFails>().sink_named("d_out");
    }
}

/// The merged input of the fan-in below (`multi-input-tasklets`): the two
/// producers carry the same payload type, so only the mapping tells the
/// join which branch a contribution came from.
#[derive(Clone, Debug)]
pub(crate) enum MergedJoinInput {
    B(String),
    C(String),
}

/// The diamond's join re-authored as a merge: same two producers, but the
/// node consumes one sum type and learns its source from the mapping.
#[derive(Default)]
pub(crate) struct MergeJoinTask;
impl Tasklet for MergeJoinTask {
    type Input = MergedJoinInput;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "bounded_merge_join_op"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: MergedJoinInput,
        _state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        let label = match input {
            MergedJoinInput::B(s) => format!("mb:{s}"),
            MergedJoinInput::C(s) => format!("mc:{s}"),
        };
        diamond_log(label.clone());
        ctx.emit(label)?;
        Ok(())
    }
}

/// A→(B,C)→merge(D): the fan-in wired as a merge, so the run must apply
/// each edge's mapping both where it accumulates a producer's released
/// batch and where a resume re-seeds from the journaled handoff.
pub(crate) struct MergeBounded;
impl BoundedOperator for MergeBounded {
    fn factory_id() -> &'static str {
        "test-merge-bounded"
    }
    fn channels(c: &ChannelBuilder) {
        let a = c.input::<String>("in").into::<DiamondA>().named("m_fan");
        let b = a.into::<DiamondB>().named("m_b");
        let cc = a.into::<DiamondC>().named("m_c");
        c.merge::<MergedJoinInput>()
            .from(b, MergedJoinInput::B)
            .from(cc, MergedJoinInput::C)
            .into::<MergeJoinTask>()
            .sink_named("m_out");
    }
}

/// The same chain whose merge mapping panics. A mapping runs in the routing
/// seam after its producing item, outside the tasklet's own invocation, so
/// without containment its unwind would take the worker with it and the
/// instance would never report.
pub(crate) struct PanickingMergeBounded;
impl BoundedOperator for PanickingMergeBounded {
    fn factory_id() -> &'static str {
        "test-panic-merge-bounded"
    }
    fn channels(c: &ChannelBuilder) {
        let a = c.input::<String>("in").into::<DiamondA>().named("pm_fan");
        let b = a.into::<DiamondB>().named("pm_b");
        let cc = a.into::<DiamondC>().named("pm_c");
        c.merge::<MergedJoinInput>()
            .from(b, |_: String| panic!("merge mapping blew up"))
            .from(cc, MergedJoinInput::C)
            .into::<MergeJoinTask>()
            .sink_named("pm_out");
    }
}

/// A plain edge `map` on an inter-tasklet edge — the same routing seam a
/// merge mapping runs on, reached without a merge.
pub(crate) struct PanickingTransformBounded;
impl BoundedOperator for PanickingTransformBounded {
    fn factory_id() -> &'static str {
        "test-panic-transform-bounded"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<DiamondA>()
            .named("pt_fan")
            .map(|_: String| -> String { panic!("edge map blew up") })
            .into::<DiamondB>()
            .sink_named("pt_out");
    }
}
