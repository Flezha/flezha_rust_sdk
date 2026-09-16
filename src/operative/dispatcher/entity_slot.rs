//! The `EntitySlot` a typed Procedure is served on: how one is built, and the
//! handout that guarantees an activation on it is answered exactly once.

use std::any::TypeId;
use std::sync::{Arc, Mutex};

use super::{EdgeFacts, EntityState, FlushWiring, ProcedureDispatcherImpl};
use crate::channel::builder::BuildEntityArgs;
use crate::channel::ChannelKind;
use crate::entity::flusher::{EntityFlusher, FlushableBuffer, TypedBuffer};
use crate::entity::{EntitySlot, ProcedureDispatcher};
use crate::operative::topology::OperativeTopology;
use crate::procedure::change::NoChange;
use crate::procedure::Procedure;

/// Answers a handout exactly once, whatever becomes of the worker holding it.
///
/// Per-entity exclusivity IS the handout: an activation that is never answered
/// retires its entity for the life of the process, silently. `Drop` closes
/// every way that could happen — an early return, a `?`, a panic unwinding
/// past the dispatcher's own guards — by abandoning, which puts the items this
/// worker did not finish back untouched and makes the entity pollable again.
pub(super) struct Handout<'a> {
    pub(super) core: &'a crate::runner::poll_core::PollCore,
    pub(super) token: u64,
    /// What the worker has not finished. The item being dispatched stays here
    /// until it resolves — the dispatch runs on a shallow clone — so an unwind
    /// mid-item re-hands it rather than losing it.
    pub(super) remaining: std::collections::VecDeque<crate::runner::poll_core::ItemAttempt>,
    pub(super) answered: bool,
}

impl<'a> Handout<'a> {
    pub(super) fn new(
        core: &'a crate::runner::poll_core::PollCore,
        token: u64,
        items: Vec<crate::runner::poll_core::ItemAttempt>,
    ) -> Self {
        Self {
            core,
            token,
            remaining: items.into(),
            answered: false,
        }
    }

    pub(super) fn next_item(&self) -> Option<crate::runner::poll_core::ItemAttempt> {
        self.remaining.front().cloned()
    }

    pub(super) fn item_done(&mut self) {
        self.remaining.pop_front();
    }

    /// The head failed retryably: it and the untouched tail ride the ladder.
    pub(super) fn retry(mut self, head: crate::runner::poll_core::ItemAttempt, error: String) {
        self.remaining.pop_front();
        let mut rest = Vec::with_capacity(self.remaining.len() + 1);
        rest.push(head);
        rest.extend(self.remaining.drain(..));
        self.answered = true;
        self.core.retry_after_failure(self.token, rest, error);
    }

    pub(super) fn complete(mut self) {
        self.answered = true;
        self.core.complete(self.token);
    }

    /// End the batch early and hand the untouched tail back. Only for items
    /// this worker did NOT run — re-handing one it already ran would execute
    /// it twice.
    pub(super) fn hand_back(&mut self) {
        self.answered = true;
        let items: Vec<_> = self.remaining.drain(..).collect();
        self.core.abandon(self.token, items);
    }
}

impl Drop for Handout<'_> {
    fn drop(&mut self) {
        if self.answered {
            return;
        }
        tracing::error!(
            target: "flezha::dispatch",
            token = self.token,
            unfinished = self.remaining.len(),
            panicking = std::thread::panicking(),
            "a poll worker left an activation unanswered; abandoning it so its entity keeps serving"
        );
        self.hand_back();
    }
}

/// Whether a Procedure's Entities join the background flusher's drain pass.
/// One predicate, applied where an Entity is built AND where an invocation has
/// to decide whether its records will ever be drained for it: a Procedure with
/// nothing to persist has no entry, so anything timers give it to persist must
/// be drained on the spot.
pub(super) fn flusher_registered<S: Procedure>(effectively_once: bool) -> bool {
    TypeId::of::<S::Change>() != TypeId::of::<NoChange>() || effectively_once
}

/// Join the background flusher's drain pass — unless this Procedure would
/// never give it anything. A `Change == NoChange` class registers nothing,
/// because registering one would spawn the drain thread for a program that
/// will never produce a record; its EntitySlot still carries an (always empty)
/// flush buffer so the dispatcher constructs a `StateHandle<'_, S>` uniformly.
/// Effectively-once operatives always register: their effect records (outputs
/// + consumed-offset, possibly with no change) drain through the same buffer.
fn register_flusher<S: Procedure>(
    parts: &flz_core::entity::SlotParts,
    flush_buffer: &Arc<Mutex<Box<dyn FlushableBuffer>>>,
    persistence_enabled: bool,
    effectively_once: bool,
) -> Option<crate::entity::flusher::FlusherEntryId> {
    flusher_registered::<S>(effectively_once).then(|| {
        EntityFlusher::instance().register(
            Arc::clone(&parts.entity_identity),
            Arc::clone(flush_buffer),
            persistence_enabled,
            Arc::clone(&parts.change_seq),
            crate::entity::header::header_template_for::<S>(),
            Arc::clone(&parts.push_deferred),
        )
    })
}

/// The dispatcher one Entity is served by: the producer- and consumer-edge
/// facts resolved out of the frozen topology once, so no invocation pays a
/// channel lookup.
fn build_dispatcher<S: Procedure>(
    args: &BuildEntityArgs<'_>,
    topology: &OperativeTopology,
    procedure_desc: &crate::operative::topology::ProcedureDescriptor,
    parts: &flz_core::entity::SlotParts,
) -> Arc<dyn ProcedureDispatcher> {
    let output_channel = procedure_desc
        .output_channel_id
        .and_then(|id| topology.channel(id));
    // Retry-then-park policy rides the procedure's input channel; an
    // unwired input keeps the defaults (park still works, channel unnamed).
    // A fan-in node has several input channels; the first wired one names
    // its DLQ entries and carries its retry ladder.
    let input_channel = procedure_desc
        .input_channel_ids
        .first()
        .and_then(|&id| topology.channel(id));
    Arc::new(ProcedureDispatcherImpl::<S>::new(
        args.procedure_id,
        EdgeFacts {
            output_channel_id: procedure_desc.output_channel_id,
            output_channel_kind: output_channel
                .map(|c| c.kind)
                .unwrap_or(ChannelKind::InterProcedure),
            output_channel_name: output_channel.map(|c| Arc::clone(&c.name)),
            output_channel_keyed: output_channel.map(|c| c.keyed).unwrap_or(false),
            input_channel_name: input_channel.map(|c| Arc::clone(&c.name)),
            retry: input_channel.map(|c| c.retry).unwrap_or_default(),
        },
        Arc::clone(&parts.entity_label),
        parts.entity_key.clone(),
        FlushWiring {
            entity_identity: Arc::clone(&parts.entity_identity),
            change_seq: Arc::clone(&parts.change_seq),
            header: crate::entity::header::header_template_for::<S>(),
            persistence_enabled: args.persistence_enabled,
            effectively_once: topology.effectively_once,
            checkpoint: topology.ack_granularity.checkpoint(),
            correction: topology.correction,
        },
    )) as Arc<dyn ProcedureDispatcher>
}

/// Build one `EntitySlot` for a Procedure. Captures S statically; the
/// channel builder collects this closure into `ProcedureEntry.build_entity`
/// during `StreamOperative::channels` and `build_topology` invokes it once per
/// entity at operative-instance startup.
pub fn build_entity_slot<S: Procedure>(args: BuildEntityArgs<'_>) -> Arc<EntitySlot> {
    // The slot's rounds are served by the typed layer's poll workers; make
    // sure they exist before the first input can be scheduled.
    crate::operative::poll_pool::ensure_poll_workers();
    // v1 cold-start path: `init_payload` is `None`. The controller change
    // wires the warm-restart path through `init_entity_with_payload`
    // and calls a richer constructor.
    let typed_state = EntityState::<S> {
        procedure: S::default(),
        state: S::init_state(args.entity_key, None),
    };

    let topology = Arc::clone(&args.topology);
    let procedure_desc = topology
        .procedure(args.procedure_id)
        .expect("build_entity_slot: procedure id missing in topology");

    let flush_buffer: Arc<Mutex<Box<dyn FlushableBuffer>>> = Arc::new(Mutex::new(Box::new(
        TypedBuffer::<S>::default(),
    )
        as Box<dyn FlushableBuffer>));

    let parts = flz_core::entity::slot_parts(
        args.instance_id.as_ref(),
        procedure_desc.name.as_ref(),
        args.entity_key,
    );
    let dispatcher = build_dispatcher::<S>(&args, &topology, procedure_desc, &parts);

    let flusher_entry_id = register_flusher::<S>(
        &parts,
        &flush_buffer,
        args.persistence_enabled,
        topology.effectively_once,
    );

    let mailbox_capacity = procedure_desc.mailbox_capacity;
    flz_core::entity::assemble_slot(
        args,
        parts,
        Box::new(typed_state),
        flush_buffer,
        dispatcher,
        flusher_entry_id,
        mailbox_capacity,
    )
}
