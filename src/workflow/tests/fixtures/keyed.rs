//! Keyed workflow classes: a `key_by` edge into a tasklet that declares no
//! keys, so its Cells are minted per key as a stream's are.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::*;
use crate::cell::restore::RestoreInitPayload;
use crate::channel::ChannelBuilder;
use crate::tasklet::change::NoChange;
use crate::tasklet::clock::WallClock;
use crate::tasklet::{StateHandle, Tasklet, TaskletCtx, TaskletError};
use crate::workflow::*;

/// Held until the running-pass sweep scenario releases it.
pub(crate) static KEYED_BLOCK_RELEASE: AtomicBool = AtomicBool::new(false);

string_task!(KeySplit, "key_split", |input, ctx| {
    for key in input.split(',') {
        ctx.emit(key.to_string())?;
    }
    Ok(())
});

string_task!(KeyedFail, "keyed_fail", |_input, _ctx| Err(
    TaskletError::Custom("keyed ship failed".into())
));

string_task!(KeyedPlain, "keyed_plain", |input, ctx| {
    ctx.emit(input)?;
    Ok(())
});

string_task!(KeyedBlock, "keyed_block", |input, ctx| {
    while !KEYED_BLOCK_RELEASE.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(1));
    }
    ctx.emit(input)?;
    Ok(())
});

/// The key the Cell was minted for, which is what its undo names.
#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) struct KeyState {
    pub(crate) key: String,
}

/// Compensable, keyed by the edge into it: emits `<key>:<item>`, and its
/// undo logs `undo-keyed:<key>` once per Cell.
#[derive(Default)]
pub(crate) struct KeyedUndo;
impl Tasklet for KeyedUndo {
    type Input = String;
    type Output = String;
    type State = KeyState;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "keyed_undo"
    }
    fn init_state(cell_key: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> KeyState {
        KeyState {
            key: cell_key.unwrap_or("").to_string(),
        }
    }
    fn run(
        &mut self,
        input: String,
        state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        ctx.emit(format!("{}:{input}", state.key))?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        _completed_output: Vec<Arc<String>>,
        state: StateHandle<'_, Self>,
        _ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        comp_log(format!("undo-keyed:{}", state.key));
        Ok(())
    }
}

/// Keyed, capped at two live Cells.
#[derive(Default)]
pub(crate) struct CappedKeyed;
impl Tasklet for CappedKeyed {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "capped_keyed"
    }
    fn max_cells() -> usize {
        2
    }
    const MAX_CELLS_OVERRIDDEN: bool = true;
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        ctx.emit(input)?;
        Ok(())
    }
}

/// Static, its keys declared out of alphabetical order, every value to both.
#[derive(Default)]
pub(crate) struct ZyStatic;
impl Tasklet for ZyStatic {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "zy_static"
    }
    fn cell_keys() -> Vec<&'static str> {
        vec!["z", "y"]
    }
    const CELLS_FOR_OVERRIDDEN: bool = true;
    fn cells_for(_input: &String) -> Vec<&'static str> {
        vec!["y", "z"]
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _input: String,
        _state: StateHandle<'_, Self>,
        _ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        Ok(())
    }
}

pub(crate) struct StaticOrderFlow;
impl WorkflowOperative for StaticOrderFlow {
    fn factory_id() -> &'static str {
        "test-static-order"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<ZyStatic>();
    }
}

fn split_then_keyed(c: &ChannelBuilder) -> crate::channel::Stream<'_, String> {
    c.input::<String>("in")
        .into::<KeySplit>()
        .key_by(|key: &String| key.clone())
        .into::<KeyedUndo>()
}

pub(crate) struct KeyedFlow;
impl WorkflowOperative for KeyedFlow {
    fn factory_id() -> &'static str {
        "test-keyed-flow"
    }
    fn channels(c: &ChannelBuilder) {
        split_then_keyed(c);
    }
}

pub(crate) struct KeyedSaga;
impl WorkflowOperative for KeyedSaga {
    fn factory_id() -> &'static str {
        "test-keyed-saga"
    }
    fn channels(c: &ChannelBuilder) {
        split_then_keyed(c).into::<KeyedFail>();
    }
}

pub(crate) struct KeyedBlockFlow;
impl WorkflowOperative for KeyedBlockFlow {
    fn factory_id() -> &'static str {
        "test-keyed-block"
    }
    fn channels(c: &ChannelBuilder) {
        split_then_keyed(c).into::<KeyedBlock>();
    }
}

pub(crate) struct CappedKeyedFlow;
impl WorkflowOperative for CappedKeyedFlow {
    fn factory_id() -> &'static str {
        "test-capped-keyed"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<KeySplit>()
            .key_by(|key: &String| key.clone())
            .into::<CappedKeyed>();
    }
}

/// A per-message loop over two keyed stages: a compensable one, then a
/// hookless one keyed by what the first emitted.
fn keyed_loop(c: &ChannelBuilder) {
    split_then_keyed(c)
        .key_by(|item: &String| item.clone())
        .into::<KeyedPlain>();
}

pub(crate) struct KeyedLoopAllOrNothing;
impl WorkflowOperative for KeyedLoopAllOrNothing {
    fn factory_id() -> &'static str {
        "test-keyed-loop-aon"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn self_terminating() -> bool {
        false
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_secs(30))
    }
    fn channels(c: &ChannelBuilder) {
        keyed_loop(c);
    }
}

pub(crate) struct KeyedLoopKeepProgress;
impl WorkflowOperative for KeyedLoopKeepProgress {
    fn factory_id() -> &'static str {
        "test-keyed-loop-kp"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn rollback_mode() -> RollbackMode {
        RollbackMode::KeepProgress
    }
    fn self_terminating() -> bool {
        false
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_secs(30))
    }
    fn channels(c: &ChannelBuilder) {
        keyed_loop(c);
    }
}
