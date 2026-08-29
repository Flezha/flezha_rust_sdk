//! The effectively-once tier's shared test surface: the lock that serialises
//! every WAL-producing test against the process-global sink, the capturing
//! sink itself, the controller-format WAL record, and the two stamped classes
//! the identity, tier and checkpoint suites are all written against.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use flezha_runner::__dispatch_internals::OperativeDispatchHandle;
use flezha_runner::channel::ChannelBuilder;
use flezha_runner::entity::header::HeaderTemplate;
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::StreamOperative;

use super::dummy_stream;

/// Serializes every test whose dispatches produce WAL frames: the
/// flush-order test installs a process-global recording sink, so a
/// concurrent WAL-producing test would leak "wal" entries into its log.
pub static WAL_TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(serde::Serialize, serde::Deserialize)]
pub struct TickChange;
impl flezha_runner::state::Change for TickChange {}

/// Keyed procedure: applies one change per input, emits the input under
/// itself as the natural key.
#[derive(Default)]
pub struct KeyedStampProcedure;
impl Procedure for KeyedStampProcedure {
    type Input = String;
    type Output = String;
    type State = i64;
    type Change = TickChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "keyed_stamp_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> i64 {
        0
    }
    fn apply_change(state: &mut i64, _change: &TickChange) {
        *state += 1;
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(TickChange);
        ctx.emit_keyed::<String>(input.clone(), input)?;
        Ok(())
    }
}

pub struct KeyedStampOperative;
impl StreamOperative for KeyedStampOperative {
    fn factory_id() -> &'static str {
        "keyed_stamp_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<KeyedStampProcedure>()
            .named("out")
            .keyed()
            .sink("kafka");
    }
}

/// Build one controller-format WAL record:
/// `[seq u64 BE][len u32 BE][header 16B][body][crc32 u32 BE]`,
/// `len = 16 + body.len()`, trailing crc over `header + body`. `template`
/// decides what the header says the body is — a change, or timer ops.
pub fn wal_record(seq: u64, change_bytes: &[u8], template: HeaderTemplate) -> Vec<u8> {
    let header = template.stamp(change_bytes);
    let mut record = Vec::new();
    record.extend_from_slice(&seq.to_be_bytes());
    record.extend_from_slice(&((16 + change_bytes.len()) as u32).to_be_bytes());
    record.extend_from_slice(&header);
    record.extend_from_slice(change_bytes);
    let mut crc_input = header.to_vec();
    crc_input.extend_from_slice(change_bytes);
    record.extend_from_slice(&crc32fast::hash(&crc_input).to_be_bytes());
    record
}

pub static TIER_SINK_FRAMES: Mutex<Vec<(String, Vec<u8>)>> = Mutex::new(Vec::new());

#[derive(Default)]
pub struct TierStampProcedure;
impl Procedure for TierStampProcedure {
    type Input = String;
    type Output = String;
    type State = i64;
    type Change = TickChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "tier_stamp_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> i64 {
        0
    }
    fn apply_change(state: &mut i64, _change: &TickChange) {
        *state += 1;
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(TickChange);
        ctx.emit_keyed::<String>(input.clone(), input)?;
        Ok(())
    }
}

pub struct TierOperative;
impl StreamOperative for TierOperative {
    fn factory_id() -> &'static str {
        "tier_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.effectively_once();
        c.input::<String>("input")
            .into::<TierStampProcedure>()
            .named("out")
            .keyed()
            .sink("kafka");
    }
}

/// Decode every `entity_effect_record` the tier sink captured, in order.
pub fn captured_effect_bodies() -> Vec<flezha_runner::entity::flusher::EffectRecordBody> {
    use flezha_runner::entity::flusher::EffectRecordBody;

    TIER_SINK_FRAMES
        .lock()
        .unwrap()
        .iter()
        .filter(|(pid, _)| pid == "entity_effect_record")
        .map(|(_, frame)| {
            let pid_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
            let body_len_off = 2 + pid_len + 8 + 16;
            let body_len =
                u32::from_be_bytes(frame[body_len_off..body_len_off + 4].try_into().unwrap())
                    as usize;
            let body: EffectRecordBody =
                postcard::from_bytes(&frame[body_len_off + 4..body_len_off + 4 + body_len])
                    .unwrap();
            body
        })
        .collect()
}

pub fn install_capturing_tier_sink() {
    use flezha_runner::entity::flusher::{SendError, SendFrameFn};
    use flezha_runner::entity::state_sink;

    TIER_SINK_FRAMES.lock().unwrap().clear();
    let sink: SendFrameFn = Arc::new(|payload_id: &str, frame: &[u8]| -> Result<(), SendError> {
        TIER_SINK_FRAMES
            .lock()
            .unwrap()
            .push((payload_id.to_string(), frame.to_vec()));
        Ok(())
    });
    state_sink::install_global_sink(sink);
}

pub fn dispatch_at(handle: &OperativeDispatchHandle, instance: &str, offset: u64, body: &str) {
    let mut md = HashMap::new();
    md.insert("channel".to_string(), "input".to_string());
    md.insert("flz.offset".to_string(), offset.to_string());
    handle.dispatch(instance, body.as_bytes().to_vec(), &md, dummy_stream());
}
