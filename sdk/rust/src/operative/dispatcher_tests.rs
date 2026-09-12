//! The typed dispatcher's own tests: the worker guard on the path it exists
//! for, the workflow collect adapter's keyed-emit contract, and a snapshot
//! round trip through the real restore path.

use std::cell::RefCell;

use super::emit_sink::WorkflowCollectAdapter;
use super::invocation::collect_failure;
use super::*;
use crate::channel::ChannelBuilder;
use crate::entity::restore::RestoreInitPayload;
use crate::operative::StreamOperative;
use crate::procedure::ctx::EmitSink;
use crate::procedure::state_handle::StateHandle;
use crate::procedure::{Procedure, ProcedureCtx, ProcedureError};
use crate::workflow::WorkflowCollectSink;

/// The worker's guard, on exactly the path it exists for. Per-entity
/// exclusivity IS the handout, so a worker that leaves without answering —
/// a panic past the dispatcher's own `catch_unwind`s, or an early return
/// like the `CloseInterval` arm with no secondary stream — would retire its
/// entity for the life of the process. The guard turns both into an
/// abandon: the items come back untouched and the next worker serves them.
#[test]
fn an_unwinding_worker_re_hands_its_activation_instead_of_retiring_the_entity() {
    use crate::runner::poll_core::{PollCore, Polled};
    use crate::runner::poll_core_tests::fixtures::{build_slot_of, push_input};

    let core = PollCore::new();
    let (_instance, slot) = build_slot_of::<KeyedOperative>("dispatcher-unwind");
    push_input(&core, &slot, 1);

    let act = match core.poll(std::time::Duration::from_millis(500)) {
        Polled::Activation(a) => a,
        _ => panic!("no activation handed out"),
    };
    let (token, _slot, items) = act.into_work().expect("work activation");
    assert_eq!(core.outstanding_count(), 1);

    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _handout = Handout::new(&core, token, items);
        panic!("the worker died holding an activation");
    }));
    assert!(unwound.is_err(), "the worker really did unwind");
    assert_eq!(core.outstanding_count(), 0, "the handout was not released");

    match core.poll(std::time::Duration::from_millis(500)) {
        Polled::Activation(again) => {
            assert_eq!(again.items().len(), 1, "the item was not re-handed");
            assert_eq!(
                again.items()[0].attempt,
                1,
                "an abandon consumes no ladder attempt"
            );
            core.complete(again.token);
        }
        _ => panic!("the entity was retired by the unwound worker"),
    }
}

#[derive(Default)]
struct KeyedProc;
impl Procedure for KeyedProc {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = crate::procedure::NoChange;
    type Clock = crate::procedure::WallClock;
    type Config = ();
    fn name() -> &'static str {
        "workflow_keyed_op"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: String,
        _state: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Ok(())
    }
}

struct KeyedOperative;
impl StreamOperative for KeyedOperative {
    fn factory_id() -> &'static str {
        "workflow_keyed_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<KeyedProc>()
            .keyed()
            .sink_named("out");
    }
}

#[derive(Default)]
struct NullCollect {
    plugin_bytes: usize,
}
impl WorkflowCollectSink for NullCollect {
    fn collect(&mut self, _value: Arc<dyn Any + Send + Sync>) {}
    fn emit_plugin_bytes(&mut self, _channel: &str, _bytes: Vec<u8>) {
        self.plugin_bytes += 1;
    }
}

/// The workflow plugin wire carries no metadata, so a keyed emit
/// through the workflow adapter must fail typed, never silently drop
/// the identity contract.
#[test]
fn workflow_adapter_rejects_keyed_emits() {
    let topology =
        crate::operative::builder::build_topology_for::<KeyedOperative>().expect("topology");
    let out_id = topology.lookup_channel_id("out");
    let mut collect = NullCollect::default();
    let mut adapter = WorkflowCollectAdapter {
        sink: &mut collect,
        output_channel_id: out_id,
        output_channel_kind: ChannelKind::PluginBound,
        output_channel_name: Some("out"),
        topology: &topology,
        effect_outputs: None,
    };
    assert!(adapter.is_keyed(), "descriptor keyed flag must surface");
    let mut ctx = ProcedureCtx::new(&mut adapter, 0, 0, b"workflow/keyed/");
    let err = ctx
        .emit_keyed::<String>("k", "v".into())
        .expect_err("keyed emit on the workflow wire must fail");
    assert!(err.to_string().contains("workflow"));
    assert_eq!(collect.plugin_bytes, 0, "nothing may reach the wire");
}

/// Under the effectively-once tier the workflow adapter accepts keyed
/// emits and captures them for the atomic effect record — never the
/// metadata-less workflow plugin wire.
#[test]
fn workflow_adapter_captures_keyed_emits_under_the_tier() {
    let topology =
        crate::operative::builder::build_topology_for::<KeyedOperative>().expect("topology");
    let out_id = topology.lookup_channel_id("out");
    let mut collect = NullCollect::default();
    let effect_outputs = RefCell::new(Vec::new());
    let mut adapter = WorkflowCollectAdapter {
        sink: &mut collect,
        output_channel_id: out_id,
        output_channel_kind: ChannelKind::PluginBound,
        output_channel_name: Some("out"),
        topology: &topology,
        effect_outputs: Some(&effect_outputs),
    };
    let mut ctx = ProcedureCtx::new(&mut adapter, 0, 0, b"workflow/keyed/");
    ctx.emit_keyed::<String>("BTC/1m", "v".into())
        .expect("keyed emit must succeed under the tier");

    assert_eq!(collect.plugin_bytes, 0, "the workflow wire carries nothing");
    let captured = effect_outputs.into_inner();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].channel, "out");
    assert_eq!(captured[0].key, "BTC/1m");
    assert_eq!(captured[0].payload, b"v".to_vec());
}

/// A workflow stage and a `compensate` hook both get a context with no
/// timer session — the two places the design excludes. The refusal is
/// structural (there is nothing to register into), and typed rather than
/// a silent drop.
#[test]
fn a_context_without_a_timer_session_refuses_rather_than_dropping() {
    let topology =
        crate::operative::builder::build_topology_for::<KeyedOperative>().expect("topology");
    let mut collect = NullCollect::default();
    let mut adapter = WorkflowCollectAdapter {
        sink: &mut collect,
        output_channel_id: topology.lookup_channel_id("out"),
        output_channel_kind: ChannelKind::PluginBound,
        output_channel_name: Some("out"),
        topology: &topology,
        effect_outputs: None,
    };
    let ctx = ProcedureCtx::new(&mut adapter, 0, 0, b"workflow/keyed/");
    let err = ctx
        .timer_after(std::time::Duration::from_secs(1))
        .expect_err("a session-less context must refuse");
    assert!(err.to_string().contains("not available"), "{err}");
    assert!(ctx.timer_at_event_time(1).is_err());
    assert!(ctx.cancel_processing_time_timer(1).is_err());
    assert!(ctx.cancel_event_time_timer(1).is_err());
}

#[derive(serde::Serialize, serde::Deserialize)]
struct AddChange(i64);
impl crate::procedure::change::Change for AddChange {}

#[derive(Default)]
struct CounterProc;
impl Procedure for CounterProc {
    type Input = String;
    type Output = String;
    type State = i64;
    type Change = AddChange;
    type Clock = crate::procedure::WallClock;
    type Config = ();
    fn name() -> &'static str {
        "snapshot_counter_op"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> i64 {
        0
    }
    fn apply_change(state: &mut i64, change: &AddChange) {
        *state += change.0;
    }
    fn run(
        &mut self,
        _: String,
        _: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Ok(())
    }
}

/// A snapshot produced by `snapshot_frame` here decodes, through the exact
/// restore path, back to the state the Entity held (task 2.6). Stamps the
/// highest seq the snapshot already contains (`change_seq - 1`), so the
/// controller's `truncate_up_to` — which keeps records strictly above it —
/// drops exactly what the snapshot folds.
#[test]
fn snapshot_frame_round_trips_through_restore() {
    use crate::entity::restore::init_entity_with_payload;

    let dispatcher = ProcedureDispatcherImpl::<CounterProc>::new(
        0,
        EdgeFacts {
            output_channel_id: None,
            output_channel_kind: ChannelKind::InterProcedure,
            output_channel_name: None,
            output_channel_keyed: false,
            input_channel_name: None,
            retry: RetryPolicy::default(),
        },
        Arc::from("inst/proc/"),
        None,
        FlushWiring {
            entity_identity: Arc::new(flz_core::flusher::encode_entity_identity(
                "inst", "proc", "",
            )),
            change_seq: Arc::new(AtomicU64::new(3)),
            header: crate::entity::header::header_template_for::<CounterProc>(),
            persistence_enabled: false,
            effectively_once: false,
            checkpoint: None,
            correction: None,
        },
    );

    let mut state: Box<dyn Any + Send> = Box::new(EntityState::<CounterProc> {
        procedure: CounterProc,
        state: 115i64,
    });
    let mut buffer: Box<dyn FlushableBuffer> = Box::new(TypedBuffer::<CounterProc>::default());

    let (frame, seq, size) = dispatcher
        .snapshot_frame(&mut state, &mut buffer, &PendingTimers::default())
        .expect("snapshot_frame must succeed for a live-typed state");
    assert_eq!(
        seq, 2,
        "change_seq 3 - 1 = the highest seq the snapshot folds"
    );

    // Extract the postcard body (header-less, exactly what the controller
    // ships back as RestoreInitState.snapshot_bytes) from the wire frame.
    let pid_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
    let body_len_off = 2 + pid_len + 8 + 16;
    let body_len =
        u32::from_be_bytes(frame[body_len_off..body_len_off + 4].try_into().unwrap()) as usize;
    let body = &frame[body_len_off + 4..body_len_off + 4 + body_len];
    assert_eq!(size, body.len(), "reported byte size is the body length");

    // The header the frame carries is what the controller stores and now
    // ships back beside the body.
    let header = &frame[2 + pid_len + 8..2 + pid_len + 8 + 16];
    let payload = RestoreInitPayload {
        snapshot_header: header,
        snapshot_bytes: body,
        change_log_bytes: &[],
    };
    let (restored, _timers, migrated) =
        init_entity_with_payload::<CounterProc>(None, Some(&payload))
            .expect("the snapshot body must decode through the restore path");
    assert!(!migrated, "same version restores without a migration");
    assert_eq!(
        restored, 115,
        "snapshot round-trips back to the original state"
    );
}

/// The four texts a failed collecting call reports. They are the saga's own
/// report and the workflow retry ladder's evidence, and one shared core now
/// composes all four — so they are pinned whole, with the author's
/// classification surviving only where the entry point has a ladder.
#[test]
fn a_collecting_failure_reports_the_four_texts_it_always_has() {
    let run = CollectShape {
        prefix: "procedure",
        author_classifies: true,
        captures_effects: true,
    };
    let compensate = CollectShape {
        prefix: "compensation for procedure",
        author_classifies: false,
        captures_effects: false,
    };
    let failure = || Ok(Err(ProcedureError::NonRetryable("boom".into())));
    let panic = || Err(Box::new("blew up") as Box<dyn Any + Send>);

    let e = collect_failure(run, "op", failure());
    let boom = "non-retryable procedure error: boom";
    assert_eq!(e.to_string(), format!("procedure op failed: {boom}"));
    assert!(
        e.non_retryable,
        "the author's classification survives `run`"
    );
    assert_eq!(
        collect_failure(run, "op", panic()).to_string(),
        "procedure op panicked: blew up"
    );

    let e = collect_failure(compensate, "op", failure());
    assert_eq!(
        e.to_string(),
        format!("compensation for procedure op failed: {boom}")
    );
    assert!(!e.non_retryable, "a compensation has no ladder of its own");
    assert_eq!(
        collect_failure(compensate, "op", panic()).to_string(),
        "compensation for procedure op panicked: blew up"
    );
}
