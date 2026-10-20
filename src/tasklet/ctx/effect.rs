#[cfg(any(test, feature = "testing"))]
use std::cell::RefCell;

use flz_core::effect_result;

use super::{Ctx, PartRoute, Phase, Tasklet, TaskletError};

/// The harness's effect results: kept by identity with their names on the live pass, served on
/// the replay pass, where a function run lands in `ran` and one never asked for stays in `kept`.
#[cfg(any(test, feature = "testing"))]
#[derive(Default)]
pub(crate) struct HarnessEffects {
    pub(crate) kept: std::collections::HashMap<String, (String, Vec<u8>)>,
    pub(crate) replaying: bool,
    pub(crate) ran: Vec<String>,
}

/// An effect's result as JSON.
fn encode<T: serde::Serialize, E: From<TaskletError>>(name: &str, value: &T) -> Result<Vec<u8>, E> {
    serde_json::to_vec(value).map_err(|e| {
        TaskletError::NonRetryable(format!("effect `{name}`: the result does not encode: {e}"))
            .into()
    })
}

/// A kept effect result read back from JSON.
fn decode<T: serde::de::DeserializeOwned, E: From<TaskletError>>(
    name: &str,
    bytes: &[u8],
) -> Result<T, E> {
    serde_json::from_slice(bytes).map_err(|e| {
        TaskletError::NonRetryable(format!(
            "effect `{name}`: the kept result does not decode: {e}"
        ))
        .into()
    })
}

/// An effect in the testing harness: the live pass keeps its result, the replay pass is served
/// it, and a function that runs on the replay pass is named.
#[cfg(any(test, feature = "testing"))]
fn in_harness<T, E>(
    effects: &RefCell<HarnessEffects>,
    name: &str,
    identity: String,
    f: impl FnOnce() -> Result<T, E>,
) -> Result<T, E>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
    E: From<TaskletError>,
{
    let served = {
        let mut effects = effects.borrow_mut();
        match effects.replaying {
            true => effects.kept.remove(&identity),
            false => effects.kept.get(&identity).cloned(),
        }
    };
    if let Some((_, kept)) = served {
        return decode(name, &kept);
    }
    let value = f()?;
    let bytes = encode(name, &value)?;
    effect_result::fits(name, &bytes).map_err(TaskletError::Effect)?;
    let mut effects = effects.borrow_mut();
    if effects.replaying {
        effects.ran.push(name.to_string());
    } else {
        effects.kept.insert(identity, (name.to_string(), bytes));
    }
    Ok(value)
}

impl<'a, S: Tasklet> Ctx<'a, S> {
    /// Keep this invocation's effect results in the testing harness.
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn with_harness_effects(mut self, effects: &'a RefCell<HarnessEffects>) -> Self {
        self.harness_effects = Some(effects);
        self
    }

    /// Run `f`, a call to anything outside the instance (a model, a tool, a
    /// service), and keep its result: when this invocation runs again (a crash
    /// re-run, a replay, a wake, a move), the call returns the kept result and
    /// `f` does not run. The result is kept, synced on the controller, before
    /// this returns; a crash between `f` finishing and that point runs `f`
    /// again, so give the outside service [`Self::stable_id`] as an idempotency
    /// key where it takes one.
    ///
    /// `name` names the call: a non-empty string of at most 255 bytes, counted
    /// per invocation, so a call of another name added before this one leaves
    /// its kept result where it was. One call runs at a time; one made from
    /// `f` is refused. The result is encoded as JSON (`T: Serialize + DeserializeOwned`).
    /// An invocation that took no input-log offset (local mode, a relay with
    /// no input log) runs `f` each time and keeps nothing.
    ///
    /// `f`'s own error is returned unchanged and nothing is kept. The call
    /// fails with `effect.bad_name`, `effect.nested` when `f` calls `effect`
    /// itself, `effect.too_large` over `FLEZHA_EFFECT_MAX_BYTES`, and
    /// `effect.not_recorded` (retryable) when the controller did not keep the
    /// result.
    pub fn effect<T, E>(&self, name: &str, f: impl FnOnce() -> Result<T, E>) -> Result<T, E>
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
        E: From<TaskletError>,
    {
        effect_result::check_name(name).map_err(TaskletError::Effect)?;
        if self.in_effect.get() {
            return Err(TaskletError::Effect(effect_result::nested(name)).into());
        }
        let ordinal = self
            .effect_ordinals
            .borrow()
            .iter()
            .find(|(counted, _)| counted == name)
            .map_or(0, |(_, next)| *next);
        let origin = flz_core::origin::InvocationOrigin {
            cell_identity: self.cell_identity,
            offset: match (self.phase, &self.part) {
                (Phase::Compensate, _) => self.undo_offset,
                (Phase::Run, PartRoute::Item(source)) => self
                    .offset
                    .or_else(|| flz_core::steps::pass_offset(source.slot)),
                (Phase::Run, _) => self.offset,
            },
        };
        self.in_effect.set(true);
        let result = self.kept_or_run(name, origin, ordinal, f);
        self.in_effect.set(false);
        result
    }

    /// Count the calls of `name` this invocation made up to `next`.
    fn counted(&self, name: &str, next: u32) {
        let mut ordinals = self.effect_ordinals.borrow_mut();
        match ordinals.iter_mut().find(|(counted, _)| counted == name) {
            Some((_, counted)) => *counted = next,
            None => ordinals.push((name.to_string(), next)),
        }
    }

    fn kept_or_run<T, E>(
        &self,
        name: &str,
        origin: flz_core::origin::InvocationOrigin<'_>,
        ordinal: u32,
        f: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E>
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
        E: From<TaskletError>,
    {
        if origin.offset.is_none() {
            return f();
        }
        #[cfg(any(test, feature = "testing"))]
        if let Some(effects) = self.harness_effects {
            let identity = origin.effect_identity(self.phase, name, ordinal);
            self.counted(name, ordinal + 1);
            return in_harness(effects, name, identity, f);
        }
        let attempt = match &self.part {
            PartRoute::Item(source) => source.attempt,
            _ => 1,
        };
        let resolved = effect_result::resolve(origin, self.phase, name, ordinal, attempt);
        self.counted(name, resolved.ordinal + 1);
        if let Some(kept) = resolved.kept {
            return decode(name, &kept);
        }
        let value = f()?;
        let bytes = encode(name, &value)?;
        match effect_result::keep(origin, name, resolved.identity, bytes) {
            Ok(None) => Ok(value),
            Ok(Some(first)) => decode(name, &first),
            Err(refusal) => Err(TaskletError::Effect(refusal).into()),
        }
    }
}
