//! Producer-first fluent graph builder (`sdk-graph-authoring`).
//!
//! `c.input::<T>("name").into::<A>().into::<B>().sink("plugin")` — `.into`
//! wires a consumer and returns that procedure's **output stream handle**
//! ([`Stream`]), so a linear pipeline is one chained expression. Branching is
//! handle reuse (hold the handle in a variable and wire it twice) or the
//! equivalent [`Stream::branch`] sugar. Fan-in is two streams naming the same
//! consumer node.
//!
//! Compile-time enforcement:
//! - `.into::<S>()` requires `S: Procedure<Input = T>` — a type-mismatched
//!   consumer fires at the call site.
//! - Channel payload types come from the producer's declared `Output`; there
//!   is no user-supplied type token for inter-procedure channels.
//! - `.sealed()` returns a finalized [`Channel`] with no further builder
//!   methods, for the author who wants "nothing follows this" in the type.
//! - Handles borrow the builder (`Stream<'b, T>` is `Copy`), so a handle
//!   cannot outlive the declaration: using one after the topology freezes at
//!   registration is a compile error, not a runtime failure.

mod issue;
mod merge;
mod state;
mod stream;

use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

pub use issue::TopologyIssue;
pub use merge::{Merge, MergeSource};
use serde::de::DeserializeOwned;
pub use state::{
    BuildEntityArgs, DeserializeStateFn, EntitiesForFn, EntityBuilder, ErasedFn, SerializeStateFn,
};
pub(crate) use state::{BuilderState, ChannelEntry, PendingEdge, ProcedureEntry};
pub use stream::{EntryStream, EntryTarget, NodeTarget, Stream, StreamTarget};

use crate::channel::ChannelKind;

/// Builder collected by `StreamOperative::channels`. Internal state turns into a
/// frozen `OperativeTopology` at `build_topology` time. Stream handles hold a
/// shared reference to the builder, so its interior is a `RefCell`.
pub struct ChannelBuilder {
    pub(crate) state: RefCell<BuilderState>,
}

impl ChannelBuilder {
    pub(crate) fn new() -> Self {
        Self {
            state: RefCell::new(BuilderState {
                channels: Vec::new(),
                procedures: Vec::new(),
                procedure_name_to_id: HashMap::new(),
                pending: Vec::new(),
                errors: Vec::new(),
                effectively_once: false,
                correction: None,
                ack_granularity: crate::operative::topology::AckGranularity::PerInput,
            }),
        }
    }

    /// Freeze: hand the collected state to `build_topology`. Consuming the
    /// builder here is what invalidates every outstanding stream handle — they
    /// borrow it, so a handle used after the freeze is a compile error.
    pub(crate) fn into_state(self) -> BuilderState {
        self.state.into_inner()
    }

    /// Whether any procedure was declared (`false` selects the workflow
    /// run-sugar path).
    pub(crate) fn has_procedures(&self) -> bool {
        !self.state.borrow().procedures.is_empty()
    }

    /// Opt this operative into late-input correction over the given window.
    ///
    /// An input whose extracted event time is behind its Entity's high-water
    /// by no more than `window_ms` is not processed as an arrival. The engine
    /// rewinds the Entity to a baseline preceding that event time, replays
    /// the retained input slice in event-time order under a bumped epoch, and
    /// keyed changelog sinks overwrite the stale rows by natural key.
    ///
    /// Requires `effectively_once()` (correction replays the durable input
    /// log) and an `EventTimeFrom` clock (there is nothing else to order by).
    /// `build_topology` refuses the declaration otherwise. The controller
    /// additionally refuses to start the operative unless its input-log hold
    /// window and snapshot age-floor both cover `window_ms` — you cannot
    /// rebuild a past you did not retain.
    ///
    /// Inputs later than the window follow [`beyond_window_policy`], which
    /// defaults to processing them as arrivals.
    ///
    /// [`beyond_window_policy`]: ChannelBuilder::beyond_window_policy
    pub fn correction_window(&self, window_ms: u64) {
        let mut st = self.state.borrow_mut();
        let beyond_window = st.correction.map(|c| c.beyond_window).unwrap_or_default();
        st.correction = Some(crate::operative::topology::CorrectionPolicy {
            window_ms,
            beyond_window,
        });
    }

    /// Fate of an input later than the declared correction window. Defaults to
    /// [`BeyondWindowPolicy::ProcessAsArrival`] — the behaviour of an operative
    /// with no correction window at all, so declaring one never silently starts
    /// dropping data.
    ///
    /// [`BeyondWindowPolicy::ProcessAsArrival`]: crate::operative::topology::BeyondWindowPolicy::ProcessAsArrival
    pub fn beyond_window_policy(&self, policy: crate::operative::topology::BeyondWindowPolicy) {
        let mut st = self.state.borrow_mut();
        let window_ms = st.correction.map(|c| c.window_ms).unwrap_or(0);
        st.correction = Some(crate::operative::topology::CorrectionPolicy {
            window_ms,
            beyond_window: policy,
        });
    }

    /// Opt this operative into the effectively-once delivery tier.
    ///
    /// In tier mode each invocation's plugin emits ship inside the atomic
    /// effect record on the reliable state sink (with the state change and
    /// consumed input offset), and the controller flushes them to plugins
    /// only after the record is fsynced. Identity is the dedup contract:
    /// `build_topology` rejects the declaration when any plugin-bound
    /// channel lacks `.keyed()`. Default is off — operatives that do not
    /// call this keep the at-least-once paths byte-identical.
    pub fn effectively_once(&self) {
        self.state.borrow_mut().effectively_once = true;
    }

    /// Advance the durable consumed-offset only at an explicit
    /// [`ProcedureCtx::checkpoint`], instead of once per input.
    ///
    /// For an operative that folds many inputs into one state push — a window
    /// that only closes on its boundary — the per-input default is not merely
    /// expensive, it is incoherent: the offset would run ahead of the state,
    /// and a restore would resume past inputs the restored state never saw.
    /// Under this granularity the changes an interval applies are held until
    /// `ctx.checkpoint()` and ship with the closing input's offset as one
    /// fsynced record, so state and offset advance together or not at all.
    ///
    /// The price is the interval: on recovery the controller re-forwards
    /// every input after the last checkpoint, and the interval's outputs are
    /// emitted again. Both are absorbed by the keyed-sink dedup the tier
    /// already requires — and the interval you choose is also the input-log
    /// retention and the recovery work you are asking for.
    ///
    /// Requires `effectively_once()`; `build_topology` refuses it otherwise.
    /// Emits made before a checkpoint are still flushed as they happen —
    /// checkpointing moves the *acknowledgement*, not the output. To hold them
    /// for the interval too, see [`outputs_at_checkpoint`].
    ///
    /// [`ProcedureCtx::checkpoint`]: crate::procedure::ProcedureCtx::checkpoint
    /// [`outputs_at_checkpoint`]: ChannelBuilder::outputs_at_checkpoint
    pub fn checkpoint_ack(&self) {
        self.checkpoint_policy(|_policy| {});
    }

    /// Close the checkpoint interval once `inputs` inputs have folded into it,
    /// instead of waiting for an explicit [`ProcedureCtx::checkpoint`].
    ///
    /// An explicit checkpoint still closes the interval early; this is the
    /// ceiling, not the only closer. Implies `checkpoint_ack()`.
    ///
    /// [`ProcedureCtx::checkpoint`]: crate::procedure::ProcedureCtx::checkpoint
    pub fn checkpoint_every(&self, inputs: u32) {
        self.checkpoint_policy(|policy| policy.every = Some(inputs));
    }

    /// Hold the interval's outputs with its changes and ship them inside its
    /// checkpoint record, which the controller flushes as one batch after that
    /// record is fsynced — one WAL append per interval instead of one per
    /// invocation.
    ///
    /// The trade is output latency: an emit is not visible downstream until
    /// its interval closes. `max_delay_ms` bounds that — the interval closes
    /// that long after the input that opened it even if nothing else arrives,
    /// so a quiet Entity never holds an output indefinitely. It is a parameter
    /// rather than an option because an unbounded hold is not a posture anyone
    /// should be able to declare by omission.
    ///
    /// Replay is exactly the mode's existing contract: an interval that did not
    /// checkpoint re-produces its outputs when it replays, a checkpointed
    /// record re-drives with identical identity, and the keyed-sink dedup the
    /// tier already requires absorbs both. Implies `checkpoint_ack()`.
    pub fn outputs_at_checkpoint(&self, max_delay_ms: u64) {
        self.checkpoint_policy(|policy| {
            policy.outputs = crate::operative::topology::CheckpointOutputs::AtCheckpoint;
            policy.max_delay_ms = Some(max_delay_ms);
        });
    }

    /// Turn the checkpoint granularity on (idempotent) and amend its policy,
    /// so each option is one call that preserves the others.
    fn checkpoint_policy(
        &self,
        amend: impl FnOnce(&mut crate::operative::topology::CheckpointPolicy),
    ) {
        use crate::operative::topology::AckGranularity;

        let mut st = self.state.borrow_mut();
        let mut policy = st.ack_granularity.checkpoint().unwrap_or_default();
        amend(&mut policy);
        st.ack_granularity = AckGranularity::Checkpoint(policy);
    }

    /// Declare the entry channel fed by the controller — the operative's
    /// external input address, so its name is explicit and mandatory.
    ///
    /// Any payload type is accepted. `Vec<u8>` is delivered unchanged and
    /// `String` UTF-8-decoded (invalid UTF-8 is dropped and logged), exactly
    /// as before; any other `T` is deserialized at the entry edge from the
    /// wire bytes with the SDK's default JSON codec (`serde_json`). A failing
    /// parse is classified like a failing invocation and rides this channel's
    /// retry→DLQ ladder. For a non-JSON wire format, use
    /// [`input_with`](ChannelBuilder::input_with).
    pub fn input<T: DeserializeOwned + Send + Sync + 'static>(
        &self,
        name: &'static str,
    ) -> EntryStream<'_, T> {
        let (wrap, parsed) = default_entry_wrap::<T>();
        self.entry_channel(name, wrap, parsed)
    }

    /// Declare the entry channel with a custom `bytes → T` parser, replacing
    /// the default JSON codec of [`input`](ChannelBuilder::input).
    ///
    /// The parser MUST be deterministic — the same bytes always produce the
    /// same value, with no ambient reads (time, randomness, I/O) — the same
    /// contract output serializers carry. A returned `Err` is classified like
    /// a failing invocation and rides this channel's retry→DLQ ladder.
    pub fn input_with<T, P>(&self, name: &'static str, parser: P) -> EntryStream<'_, T>
    where
        T: Send + Sync + 'static,
        P: Fn(Vec<u8>) -> Result<T, String> + Send + Sync + 'static,
    {
        let wrap: crate::channel::EntryWrapFn = Arc::new(move |bytes: Vec<u8>| {
            parser(bytes).map(|v| Arc::new(v) as Arc<dyn Any + Send + Sync>)
        });
        self.entry_channel(name, wrap, true)
    }

    /// Open a **merged** subscription: one procedure fed by two or more source
    /// channels, each supplying a mapping from its payload into the
    /// procedure's `Input` sum type (`multi-input-procedures`).
    ///
    /// ```ignore
    /// c.merge::<SignalInput>()
    ///     .from(ticker, SignalInput::Ticker)
    ///     .from(config, SignalInput::Config)
    ///     .into::<DetectorProcedure>();
    /// ```
    ///
    /// Sources may be entry streams (sealed payloads) or another procedure's
    /// output stream, freely mixed — on an **stream** operative. A workflow
    /// operative is seeded from its spawn args through one entry channel and
    /// nothing on that path says which channel a payload arrived on, so a
    /// workflow merge over an entry channel is refused at registration
    /// (`BuildWorkflowError::MergeFromEntryChannel`); its sources are the
    /// output streams of two or more of its procedures.
    ///
    /// `entities_for` and the merge's own `key_by` are evaluated on the
    /// **mapped** value, so per-source entity mode is encoded in the sum type.
    /// A source handle may carry its own transforms, which run before that
    /// source's mapping; keying is declared once, on the merge. Per-source FIFO
    /// order is preserved; nothing is guaranteed across sources — correlate in
    /// the procedure's own state.
    ///
    /// The mappings MUST be pure: they run on live delivery and again on WAL
    /// replay, and a replay that disagreed with the live run would restore a
    /// state the live run never had. They must also be total — a mapping
    /// returns `I`, not `Result<I, _>`, so unlike an entry parser it has no
    /// error to hand the channel's retry→DLQ ladder. Do the fallible work in
    /// the procedure, on the sum type.
    ///
    /// A panicking mapping is contained everywhere it can be: on an
    /// inter-procedure source it is caught as the producing procedure's panic
    /// (stream) or turned into a terminal chain error the saga compensates
    /// (workflow). The one unguarded seam is a stream entry-channel source,
    /// where the mapping runs on the wire-reader path — the same exposure the
    /// entry parser there already carries. `docs/sdk/multi_input_procedures.md`
    /// tabulates it.
    ///
    /// A source's payload must be [`Clone`]: the mapping is handed the
    /// channel's value by reference and clones it, where a plain `.into`
    /// forwards the existing `Arc`.
    ///
    /// `Procedure::Input == I` is enforced at the `.into::<Proc>()` call site by
    /// the compiler. Fewer than two sources, the same channel twice, or a
    /// source whose edge into this procedure is already wired all fail at
    /// registration ([`TopologyIssue::MergeTooFewSources`] /
    /// [`TopologyIssue::DuplicateMergeSource`] /
    /// [`TopologyIssue::EdgeAlreadyWired`]).
    pub fn merge<I: Send + Sync + 'static>(&self) -> Merge<'_, I> {
        Merge {
            builder: self,
            sources: Vec::new(),
            declared: 0,
            key: None,
            duplicate_key: false,
            _phantom: PhantomData,
        }
    }

    fn entry_channel<T: Send + Sync + 'static>(
        &self,
        name: &'static str,
        wrap: crate::channel::EntryWrapFn,
        entry_parsed: bool,
    ) -> EntryStream<'_, T> {
        let mut st = self.state.borrow_mut();
        let id = st.push_channel(name, ChannelKind::Entry);
        let entry = &mut st.channels[id as usize];
        entry.wrap_input = Some(wrap);
        entry.entry_parsed = entry_parsed;
        EntryStream {
            builder: self,
            target: id,
            pending: None,
            _phantom: PhantomData,
        }
    }
}

/// The default entry decode for `T`: passthrough for `Vec<u8>`, UTF-8 for
/// `String` (both byte-identical to the pre-typed-entry contract), the SDK's
/// default JSON codec otherwise. The bool is the "real parser" flag driving
/// the retry→DLQ classification of a failing decode.
fn default_entry_wrap<T: DeserializeOwned + Send + Sync + 'static>(
) -> (crate::channel::EntryWrapFn, bool) {
    if TypeId::of::<T>() == TypeId::of::<Vec<u8>>() {
        (
            Arc::new(|bytes: Vec<u8>| Ok(Arc::new(bytes) as Arc<dyn Any + Send + Sync>)),
            false,
        )
    } else if TypeId::of::<T>() == TypeId::of::<String>() {
        (
            Arc::new(|bytes: Vec<u8>| match String::from_utf8(bytes) {
                Ok(s) => Ok(Arc::new(s) as Arc<dyn Any + Send + Sync>),
                Err(e) => Err(format!("entry channel: invalid UTF-8: {}", e)),
            }),
            false,
        )
    } else {
        (
            Arc::new(|bytes: Vec<u8>| {
                serde_json::from_slice::<T>(&bytes)
                    .map(|v| Arc::new(v) as Arc<dyn Any + Send + Sync>)
                    .map_err(|e| format!("entry channel: JSON deserialization failed: {e}"))
            }),
            true,
        )
    }
}
