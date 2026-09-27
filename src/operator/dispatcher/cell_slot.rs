//! The `CellSlot` a typed Tasklet is served on: how one is built, and the
//! handout that guarantees an activation on it is answered exactly once.

use std::any::TypeId;
use std::sync::{Arc, Mutex};

use super::{CellState, EdgeFacts, FlushWiring, TaskletDispatcherImpl};
use crate::cell::flusher::{CellFlusher, FlushableBuffer, TypedBuffer};
use crate::cell::{CellSlot, TaskletDispatcher};
use crate::channel::builder::BuildCellArgs;
use crate::operator::topology::OperatorTopology;
use crate::tasklet::change::NoChange;
use crate::tasklet::Tasklet;

/// Answers a handout exactly once, whatever becomes of the worker holding it.
///
/// Per-cell exclusivity IS the handout: an activation that is never answered
/// retires its cell for the life of the process, silently. `Drop` closes
/// every way that could happen — an early return, a `?`, a panic unwinding
/// past the dispatcher's own guards — by abandoning, which puts the items this
/// worker did not finish back untouched and makes the cell pollable again.
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
            "a poll worker left an activation unanswered; abandoning it so its cell keeps serving"
        );
        self.hand_back();
    }
}

/// Whether a Tasklet's Cells join the background flusher's drain pass.
/// One predicate, applied where a Cell is built AND where an invocation has
/// to decide whether its records will ever be drained for it: a Tasklet with
/// nothing to persist has no entry, so anything timers give it to persist must
/// be drained on the spot.
pub(super) fn flusher_registered<S: Tasklet>(records_effects: bool) -> bool {
    TypeId::of::<S::Change>() != TypeId::of::<NoChange>() || records_effects
}

/// Join the background flusher's drain pass — unless this Tasklet would
/// never give it anything. A `Change == NoChange` class registers nothing,
/// because registering one would spawn the drain thread for a program that
/// will never produce a record; its CellSlot still carries an (always empty)
/// flush buffer so the dispatcher constructs a `StateHandle<'_, S>` uniformly.
/// A Cell that records effects always registers: its effect records (outputs
/// + consumed-offset, possibly with no change) drain through the same buffer.
fn register_flusher<S: Tasklet>(
    parts: &flz_core::cell::SlotParts,
    prefix: &Option<Arc<flz_core::prefix::PrefixLedger>>,
    flush_buffer: &Arc<Mutex<Box<dyn FlushableBuffer>>>,
    persistence_enabled: bool,
    records_effects: bool,
) -> Option<crate::cell::flusher::FlusherEntryId> {
    flusher_registered::<S>(records_effects).then(|| {
        CellFlusher::instance().register(
            Arc::clone(&parts.cell_identity),
            prefix.clone(),
            Arc::clone(flush_buffer),
            persistence_enabled,
            Arc::clone(&parts.change_seq),
            crate::cell::header::header_template_for::<S>(),
            Arc::clone(&parts.push_deferred),
        )
    })
}

/// The dispatcher one Cell is served by: the producer- and consumer-edge
/// facts resolved out of the frozen topology once, so no invocation pays a
/// channel lookup.
fn build_dispatcher<S: Tasklet>(
    args: &BuildCellArgs<'_>,
    topology: &OperatorTopology,
    tasklet_desc: &crate::operator::topology::TaskletDescriptor,
    parts: &flz_core::cell::SlotParts,
) -> Arc<dyn TaskletDispatcher> {
    let output_channel = tasklet_desc
        .output_channel_id
        .and_then(|id| topology.channel(id));
    // Retry-then-park policy rides the tasklet's input channel; an
    // unwired input keeps the defaults (park still works, channel unnamed).
    // A fan-in node has several input channels; the first wired one names
    // its DLQ entries and carries its retry ladder.
    let input_channel = tasklet_desc
        .input_channel_ids
        .first()
        .and_then(|&id| topology.channel(id));
    Arc::new(TaskletDispatcherImpl::<S>::new(
        args.tasklet_id,
        EdgeFacts {
            output_channel_id: tasklet_desc.output_channel_id,
            output_channel_name: output_channel.map(|c| Arc::clone(&c.name)),
            output_channel_keyed: output_channel.map(|c| c.keyed).unwrap_or(false),
            input_channel_name: input_channel.map(|c| Arc::clone(&c.name)),
            retry: input_channel.map(|c| c.retry).unwrap_or_default(),
        },
        Arc::clone(&parts.cell_label),
        parts.cell_key.clone(),
        FlushWiring {
            cell_identity: Arc::clone(&parts.cell_identity),
            change_seq: Arc::clone(&parts.change_seq),
            header: crate::cell::header::header_template_for::<S>(),
            persistence_enabled: args.persistence_enabled,
            records_effects: topology.records_effects(args.persistence_enabled),
            checkpoint: topology.ack_granularity.checkpoint(),
            correction: topology.correction,
            bounded: topology.bounded.is_some(),
        },
    )) as Arc<dyn TaskletDispatcher>
}

/// Build one `CellSlot` for a Tasklet. Captures S statically; the
/// channel builder collects this closure into `TaskletEntry.build_cell`
/// during `UnboundedOperator::channels` and `build_topology` invokes it once per
/// cell at operator-instance startup.
pub fn build_cell_slot<S: Tasklet>(args: BuildCellArgs<'_>) -> Arc<CellSlot> {
    // The slot's rounds are served by the typed layer's poll workers; make
    // sure they exist before the first input can be scheduled.
    crate::operator::poll_pool::ensure_poll_workers();
    // v1 cold-start path: `init_payload` is `None`. The controller change
    // wires the warm-restart path through `init_cell_with_payload`
    // and calls a richer constructor.
    let typed_state = CellState::<S> {
        tasklet: S::default(),
        state: S::init_state(args.cell_key, None),
    };

    let topology = Arc::clone(&args.topology);
    let tasklet_desc = topology
        .tasklet(args.tasklet_id)
        .expect("build_cell_slot: tasklet id missing in topology");

    let flush_buffer: Arc<Mutex<Box<dyn FlushableBuffer>>> = Arc::new(Mutex::new(Box::new(
        TypedBuffer::<S>::default(),
    )
        as Box<dyn FlushableBuffer>));

    let parts = flz_core::cell::slot_parts(
        args.instance_id.as_ref(),
        tasklet_desc.name.as_ref(),
        args.cell_key,
    );
    let dispatcher = build_dispatcher::<S>(&args, &topology, tasklet_desc, &parts);

    let flusher_entry_id = register_flusher::<S>(
        &parts,
        &args.prefix,
        &flush_buffer,
        args.persistence_enabled,
        topology.records_effects(args.persistence_enabled),
    );

    let mailbox_capacity = tasklet_desc.mailbox_capacity;
    flz_core::cell::assemble_slot(
        args,
        parts,
        Box::new(typed_state),
        flush_buffer,
        dispatcher,
        flusher_entry_id,
        mailbox_capacity,
    )
}
