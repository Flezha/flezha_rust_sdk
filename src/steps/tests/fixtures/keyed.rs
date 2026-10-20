//! Keyed steps classes: a `key_by` edge into a tasklet that declares no
//! keys, so its Cells are minted per key as a flow relay's are.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::*;
use crate::channel::ChannelBuilder;
use crate::steps::*;
use crate::tasklet::{Ctx, Tasklet, TaskletError};
use crate::{Durable, Relay, RelaySettings, Run, SleepAfter};

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
#[flezha_runner::tasklet(name = "keyed_undo")]
impl Tasklet for KeyedUndo {
    type Input = String;
    type Output = String;
    type State = KeyState;
    fn init_state(cell_key: Option<&str>) -> KeyState {
        KeyState {
            key: cell_key.unwrap_or("").to_string(),
        }
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("{}:{input}", ctx.state().key))?;
        Ok(())
    }
    fn compensate(
        &mut self,
        _completed_output: Vec<Arc<String>>,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        comp_log(format!("undo-keyed:{}", ctx.state().key));
        Ok(())
    }
}

/// Keyed, capped at two live Cells.
#[derive(Default)]
pub(crate) struct CappedKeyed;
#[flezha_runner::tasklet(name = "capped_keyed")]
impl Tasklet for CappedKeyed {
    type Input = String;
    type Output = String;
    fn max_cells() -> usize {
        2
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input)?;
        Ok(())
    }
}

/// Static, its keys declared out of alphabetical order, every value to both.
#[derive(Default)]
pub(crate) struct ZyStatic;
#[flezha_runner::tasklet(name = "zy_static", cell_keys = ["z", "y"])]
impl Tasklet for ZyStatic {
    type Input = String;
    type Output = String;
    fn cells_for(_input: &String) -> Vec<&'static str> {
        vec!["y", "z"]
    }
    fn run(&mut self, _input: String, _ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

pub(crate) struct StaticOrderFlow;
impl Relay for StaticOrderFlow {
    fn factory_id() -> &'static str {
        "test-static-order"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<ZyStatic>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            ..RelaySettings::default()
        }
    }
}

fn split_then_keyed(c: &ChannelBuilder) -> crate::channel::Stream<'_, String> {
    c.input::<String>("in")
        .into::<KeySplit>()
        .key_by(|key: &String| key.clone())
        .into::<KeyedUndo>()
}

pub(crate) struct KeyedFlow;
impl Relay for KeyedFlow {
    fn factory_id() -> &'static str {
        "test-keyed-flow"
    }
    fn channels(c: &ChannelBuilder) {
        split_then_keyed(c);
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            ..RelaySettings::default()
        }
    }
}

pub(crate) struct KeyedSaga;
impl Relay for KeyedSaga {
    fn factory_id() -> &'static str {
        "test-keyed-saga"
    }
    fn channels(c: &ChannelBuilder) {
        split_then_keyed(c).into::<KeyedFail>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            ..RelaySettings::default()
        }
    }
}

pub(crate) struct KeyedBlockFlow;
impl Relay for KeyedBlockFlow {
    fn factory_id() -> &'static str {
        "test-keyed-block"
    }
    fn channels(c: &ChannelBuilder) {
        split_then_keyed(c).into::<KeyedBlock>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            ..RelaySettings::default()
        }
    }
}

pub(crate) struct CappedKeyedFlow;
impl Relay for CappedKeyedFlow {
    fn factory_id() -> &'static str {
        "test-capped-keyed"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<KeySplit>()
            .key_by(|key: &String| key.clone())
            .into::<CappedKeyed>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            ..RelaySettings::default()
        }
    }
}

/// A per-message loop over two keyed stages: a compensable one, then a
/// hookless one keyed by what the first emitted.
fn keyed_loop(c: &ChannelBuilder) {
    split_then_keyed(c)
        .key_by(|item: &String| item.clone())
        .into::<KeyedPlain>();
}

pub(crate) struct KeyedLoopUndoAll;
impl Relay for KeyedLoopUndoAll {
    fn factory_id() -> &'static str {
        "test-keyed-loop-aon"
    }
    fn channels(c: &ChannelBuilder) {
        keyed_loop(c);
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_secs(30)),
            durable: Durable::Local {
                sleep_after: SleepAfter::After(LONG_SLEEP),
            },
            ..RelaySettings::default()
        }
    }
}

pub(crate) struct KeyedLoopKeepProgress;
impl Relay for KeyedLoopKeepProgress {
    fn factory_id() -> &'static str {
        "test-keyed-loop-kp"
    }
    fn channels(c: &ChannelBuilder) {
        keyed_loop(c);
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::KeepProgress,
            },
            max_duration: Some(Duration::from_secs(30)),
            durable: Durable::Local {
                sleep_after: SleepAfter::After(LONG_SLEEP),
            },
            ..RelaySettings::default()
        }
    }
}

/// A per-input loop keyed on its entry edge: the shape a partition group of
/// steps relay members has, one Cell per message key.
pub(crate) struct KeyedEntryLoop;
impl Relay for KeyedEntryLoop {
    fn factory_id() -> &'static str {
        "test-keyed-entry-loop"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .key_by(|item: &String| item.clone())
            .into::<KeyedPlain>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_secs(30)),
            durable: Durable::Local {
                sleep_after: SleepAfter::After(LONG_SLEEP),
            },
            ..RelaySettings::default()
        }
    }
}
