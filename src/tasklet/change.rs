//! The named bounds on the values the runtime writes down: `Change` and its
//! `NoChange` zero-variant default, the `CoalesceKey` newtype (per
//! `relay-state-persistence/design.md` Decisions 3 and 4), and
//! `JournalValue`, the bound a Tasklet's `Output` carries.
//!
//! `Change<S>` is a change a tasklet records against its state `S`: it folds
//! itself with `apply_to`, and may name a `coalesce_key` so the per-Cell
//! flusher keeps only the latest record per key.
//!
//! `NoChange` is the uninhabited change of a tasklet that keeps no state. The
//! runtime detects it via `TypeId` and skips flush-buffer registration entirely.

use std::hash::{Hash, Hasher};

use serde::de::DeserializeOwned;
use serde::Serialize;

/// Persistent state-mutation record on the state `S`. Implementors describe
/// one logical change; the per-Cell flusher serializes these records and hands
/// the bytes to the controller's WAL, and a restart folds them back through
/// [`Self::apply_to`] in seq order.
///
/// An enum's variants are encoded by index, so a new variant goes at the end.
pub trait Change<S>: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// Fold this change into `state`. MUST be deterministic: the live run,
    /// every replay and every restore call it on the same state and must
    /// reach the same result, which `TaskletTestHarness::replay_equals`
    /// verifies.
    fn apply_to(&self, state: &mut S);

    /// Coalescing identifier. Records sharing a non-`None` key within a
    /// single linger window collapse to the most recent record. Records
    /// returning `None` are retained verbatim.
    fn coalesce_key(&self) -> Option<CoalesceKey> {
        None
    }
}

/// The change of a tasklet that keeps no state: uninhabited, so it can never
/// be recorded. Detected at relay-build time via `TypeId` so the runtime
/// skips flush-buffer allocation for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoChange {}

impl Serialize for NoChange {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        match *self {}
    }
}

impl<'de> serde::Deserialize<'de> for NoChange {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "NoChange is uninhabited; deserialization is structurally unreachable",
        ))
    }
}

impl<S> Change<S> for NoChange {
    fn apply_to(&self, _state: &mut S) {
        match *self {}
    }

    fn coalesce_key(&self) -> Option<CoalesceKey> {
        match *self {}
    }
}

/// A value the runtime can write down and read back: the journal codec of
/// every inter-tasklet channel, carried by the type rather than declared per
/// channel.
///
/// [`Tasklet::Output`](crate::tasklet::Tasklet::Output) carries this
/// bound because one tasklet's output is the handoff to the next, and a
/// durable structured steps journals that handoff at its commit barrier so
/// a resumed run can seed the next tasklet instead of re-running the chain
/// from the top. Whether a class journals is not settled where it is compiled
/// — a user overrides persistence per class at deploy time — so the bound
/// is unconditional, like the one `Tasklet::State` already carries.
///
/// Nothing implements this by hand. The blanket implementation covers every
/// `Serialize + DeserializeOwned` type, which is what
/// `#[derive(serde::Serialize, serde::Deserialize)]` produces, and it leaves
/// no room for a second implementation. The trait exists under its own name so
/// that an output which does not qualify is refused in the SDK's own words
/// instead of in serde's, which names traits the author never wrote.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be written down, so it cannot be a tasklet's `Output`",
    label = "this output has no byte form",
    note = "a tasklet's output is the handoff to the next tasklet, and a durable steps \
            journals that handoff at its commit barrier and re-seeds from it on crash-resume",
    note = "add `#[derive(serde::Serialize, serde::Deserialize)]` to `{Self}`"
)]
pub trait JournalValue: Sized {
    /// The handoff's byte form. Postcard, the same wire format the effect
    /// records use.
    fn to_journal_bytes(&self) -> Result<Vec<u8>, String>;

    /// Read back what [`Self::to_journal_bytes`] wrote.
    fn from_journal_bytes(bytes: &[u8]) -> Result<Self, String>;
}

#[diagnostic::do_not_recommend]
impl<T: Serialize + DeserializeOwned> JournalValue for T {
    fn to_journal_bytes(&self) -> Result<Vec<u8>, String> {
        postcard::to_allocvec(self).map_err(|e| format!("inter-task codec encode failed: {e}"))
    }

    fn from_journal_bytes(bytes: &[u8]) -> Result<Self, String> {
        postcard::from_bytes(bytes).map_err(|e| format!("inter-task codec decode failed: {e}"))
    }
}

/// Opaque coalescing key. Constructed via `CoalesceKey::from_hash(&value)`
/// for any `Hash` source. The runtime hashes the source value with
/// `DefaultHasher` (SipHash-13) and stores the resulting `u64` so the
/// drain-time coalescer can index a `HashMap<CoalesceKey, _>` without
/// borrowing the record's fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CoalesceKey(u64);

impl CoalesceKey {
    pub fn from_hash<H: Hash>(value: &H) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        value.hash(&mut hasher);
        Self(hasher.finish())
    }

    /// Internal accessor for the raw 64-bit hash. Exposed for testing and
    /// telemetry; downstream callers should treat the value as opaque.
    #[doc(hidden)]
    pub fn raw(self) -> u64 {
        self.0
    }
}

impl<T: Hash> From<&T> for CoalesceKey {
    fn from(value: &T) -> Self {
        Self::from_hash(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalesce_key_from_hash_is_stable_for_same_input() {
        let a = CoalesceKey::from_hash(&("M1", 1234u64));
        let b = CoalesceKey::from_hash(&("M1", 1234u64));
        assert_eq!(a, b);
    }

    #[test]
    fn coalesce_key_distinguishes_different_inputs() {
        let a = CoalesceKey::from_hash(&("M1", 1234u64));
        let b = CoalesceKey::from_hash(&("M5", 1234u64));
        assert_ne!(a, b);
    }

    #[test]
    fn no_change_default_coalesce_key_is_unreachable_in_practice() {
        // Cannot construct a NoChange — the impl is structurally
        // unreachable. This test merely documents that the type compiles.
        let _: fn(&NoChange) -> Option<CoalesceKey> = |c| Change::<()>::coalesce_key(c);
    }
}
