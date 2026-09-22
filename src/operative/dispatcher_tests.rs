//! The typed dispatcher's own tests: the worker guard on the path it exists
//! for, a context with no timer session, and a snapshot round trip through
//! the real restore path.

use super::*;
use crate::cell::restore::RestoreInitPayload;
use crate::channel::ChannelBuilder;
use crate::operative::StreamOperative;
use crate::tasklet::ctx::EmitSink;
use crate::tasklet::state_handle::StateHandle;
use crate::tasklet::{Tasklet, TaskletCtx, TaskletError};

/// The worker's guard, on exactly the path it exists for. Per-cell
/// exclusivity IS the handout, so a worker that leaves without answering —
/// a panic past the dispatcher's own `catch_unwind`s, or an early return
/// like the `CloseInterval` arm with no secondary stream — would retire its
/// cell for the life of the process. The guard turns both into an
/// abandon: the items come back untouched and the next worker serves them.
#[test]
fn an_unwinding_worker_re_hands_its_activation_instead_of_retiring_the_cell() {
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
        _ => panic!("the cell was retired by the unwound worker"),
    }
}

#[derive(Default)]
struct KeyedTask;
impl Tasklet for KeyedTask {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = crate::tasklet::NoChange;
    type Clock = crate::tasklet::WallClock;
    type Config = ();
    fn name() -> &'static str {
        "workflow_keyed_op"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: String,
        _state: StateHandle<'_, Self>,
        _: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
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
            .into::<KeyedTask>()
            .keyed()
            .sink_named("out");
    }
}

/// An emit sink that takes nothing: what a context with no timer session
/// needs to exist.
struct NoSink;
impl EmitSink for NoSink {
    fn emit(
        &mut self,
        _value: Arc<dyn Any + Send + Sync>,
        _meta: Option<std::collections::HashMap<String, String>>,
        _key: Option<String>,
    ) -> Result<(), TaskletError> {
        Ok(())
    }

    fn channel_name(&self) -> &str {
        "out"
    }

    fn has_plugin_sink(&self) -> bool {
        false
    }

    fn is_keyed(&self) -> bool {
        false
    }
}

/// A workflow Cell's context carries no timer session — timers do not fire
/// inside a workflow yet. The refusal is structural (there is nothing to
/// register into), and typed rather than a silent drop.
#[test]
fn a_context_without_a_timer_session_refuses_rather_than_dropping() {
    let mut sink = NoSink;
    let ctx = TaskletCtx::new(&mut sink, 0, 0, b"workflow/keyed/");
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
impl crate::tasklet::change::Change for AddChange {}

#[derive(Default)]
struct CounterTask;
impl Tasklet for CounterTask {
    type Input = String;
    type Output = String;
    type State = i64;
    type Change = AddChange;
    type Clock = crate::tasklet::WallClock;
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
        _: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        Ok(())
    }
}

/// A snapshot produced by `snapshot_frame` here decodes, through the exact
/// restore path, back to the state the Cell held (task 2.6). Stamps the
/// highest seq the snapshot already contains (`change_seq - 1`), so the
/// controller's `truncate_up_to` — which keeps records strictly above it —
/// drops exactly what the snapshot folds.
#[test]
fn snapshot_frame_round_trips_through_restore() {
    use crate::cell::restore::init_cell_with_payload;

    let dispatcher = TaskletDispatcherImpl::<CounterTask>::new(
        0,
        EdgeFacts {
            output_channel_id: None,
            output_channel_name: None,
            output_channel_keyed: false,
            input_channel_name: None,
            retry: RetryPolicy::default(),
        },
        Arc::from("inst/task/"),
        None,
        FlushWiring {
            cell_identity: Arc::new(flz_core::flusher::encode_cell_identity("inst", "task", "")),
            change_seq: Arc::new(AtomicU64::new(3)),
            header: crate::cell::header::header_template_for::<CounterTask>(),
            persistence_enabled: false,
            effectively_once: false,
            checkpoint: None,
            correction: None,
            workflow: false,
        },
    );

    let mut state: Box<dyn Any + Send> = Box::new(CellState::<CounterTask> {
        tasklet: CounterTask,
        state: 115i64,
    });
    let mut buffer: Box<dyn FlushableBuffer> = Box::new(TypedBuffer::<CounterTask>::default());

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
    let (restored, _timers, migrated) = init_cell_with_payload::<CounterTask>(None, Some(&payload))
        .expect("the snapshot body must decode through the restore path");
    assert!(!migrated, "same version restores without a migration");
    assert_eq!(
        restored, 115,
        "snapshot round-trips back to the original state"
    );
}
