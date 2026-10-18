//! The typed half of the per-Cell flush path: [`TypedBodies`], the codec
//! marker that hands `flz_core`'s shared [`EffectBuffer`] the author's own
//! `S::Change`, plus drain-time coalescing and the change-frame builder. The
//! erased flusher — registry, linger thread, `flush_one`, frame assembly —
//! lives in `flz_core::flusher` and is re-exported here.

use std::collections::HashMap;
use std::marker::PhantomData;

pub use flz_core::flusher::*;
use flz_core::header::HeaderTemplate;

use crate::tasklet::change::{Change, CoalesceKey};
use crate::tasklet::Tasklet;

/// How a typed class's changes become record bodies: coalesced on the
/// author's `coalesce_key`, then postcard-encoded — the codec
/// `cell/restore.rs` folds back.
pub struct TypedBodies<S: Tasklet>(PhantomData<fn() -> S>);

impl<S: Tasklet> Bodies for TypedBodies<S> {
    type Change = S::Change;

    fn coalesce(changes: Vec<S::Change>) -> Vec<S::Change> {
        coalesce::<S::State, _>(changes)
    }

    fn into_bodies(changes: Vec<S::Change>) -> Vec<Vec<u8>> {
        changes.iter().map(encode_change).collect()
    }
}

/// One change's record body: the bytes the restore fold decodes back.
pub(crate) fn encode_change<C: serde::Serialize>(change: &C) -> Vec<u8> {
    postcard::to_allocvec(change)
        .expect("postcard serialization of Change failed; Change must be Serialize")
}

/// The per-Cell flush buffer on a Tasklet's own change type. The
/// dispatcher downcasts the type-erased `dyn FlushableBuffer` back to this to
/// reach `&mut Vec<S::Change>` for `Ctx::new`.
pub type TypedBuffer<S> = EffectBuffer<TypedBodies<S>>;

/// Drain-time coalesce: keep the latest record per `coalesce_key`, preserve
/// order of kept records, and retain `coalesce_key() == None` records
/// verbatim. Nothing is allocated until the first keyed record appears, so a
/// buffer with no keys at all comes back as the same allocation.
pub fn coalesce<S, C: Change<S>>(buf: Vec<C>) -> Vec<C> {
    let mut latest: HashMap<CoalesceKey, usize> = HashMap::new();
    // Empty until the first key: also the flag that says nothing is collapsed.
    let mut keep_mask: Vec<bool> = Vec::new();
    for (i, c) in buf.iter().enumerate() {
        let Some(k) = c.coalesce_key() else {
            continue;
        };
        if keep_mask.is_empty() {
            keep_mask = vec![true; buf.len()];
            latest.reserve(buf.len() - i);
        }
        if let Some(prev_i) = latest.insert(k, i) {
            keep_mask[prev_i] = false;
        }
    }
    if keep_mask.is_empty() {
        return buf;
    }
    let mut out = Vec::with_capacity(buf.len());
    for (i, c) in buf.into_iter().enumerate() {
        if keep_mask[i] {
            out.push(c);
        }
    }
    out
}

/// Build one wire frame matching `Flezha.State.Wire.decode_frame/1`.
pub fn build_cell_state_change_frame<C: serde::Serialize>(
    cell_identity: &[u8],
    seq: u64,
    header: &HeaderTemplate,
    change: &C,
) -> Vec<u8> {
    let change_bytes = encode_change(change);
    let header_bytes = header.stamp(&change_bytes);
    build_cell_frame(cell_identity, seq, &header_bytes, &change_bytes)
}

/// A cell frame split back into its parts: the inverse of
/// [`build_cell_frame`], for tests that read what a run shipped.
#[cfg(test)]
pub(crate) struct CellFrame<'a> {
    pub identity: &'a [u8],
    pub seq: u64,
    pub header: &'a [u8],
    pub body: &'a [u8],
}

#[cfg(test)]
pub(crate) fn split_cell_frame(frame: &[u8]) -> Option<CellFrame<'_>> {
    let id_len = u16::from_be_bytes(frame.get(..2)?.try_into().ok()?) as usize;
    let at = 2 + id_len;
    let body_len = u32::from_be_bytes(frame.get(at + 24..at + 28)?.try_into().ok()?) as usize;
    Some(CellFrame {
        identity: frame.get(2..at)?,
        seq: u64::from_be_bytes(frame.get(at..at + 8)?.try_into().ok()?),
        header: frame.get(at + 8..at + 24)?,
        body: frame.get(at + 28..at + 28 + body_len)?,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use flz_core::header::FORMAT_ID_EFFECT_RECORD;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct TestChange {
        key: Option<u8>,
        ts: u64,
        value: i64,
    }
    impl Change<()> for TestChange {
        fn apply_to(&self, _: &mut ()) {}

        fn coalesce_key(&self) -> Option<CoalesceKey> {
            self.key.map(|k| CoalesceKey::from_hash(&k))
        }
    }

    #[test]
    fn coalesce_collapses_keyed_records_to_latest() {
        let buf: Vec<TestChange> = (0..50)
            .map(|i| TestChange {
                key: Some(7),
                ts: i,
                value: i as i64,
            })
            .collect();
        let out = coalesce::<(), _>(buf);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].ts, 49);
        assert_eq!(out[0].value, 49);
    }

    #[test]
    fn coalesce_preserves_no_key_records_verbatim() {
        let buf: Vec<TestChange> = (0..50)
            .map(|i| TestChange {
                key: None,
                ts: i,
                value: i as i64,
            })
            .collect();
        let out = coalesce::<(), _>(buf);
        assert_eq!(out.len(), 50);
        for (i, c) in out.iter().enumerate() {
            assert_eq!(c.ts, i as u64);
        }
    }

    /// With no record carrying a key there is nothing to collapse: the input
    /// comes back as the same allocation, which is what proves the map and
    /// the mask were never allocated.
    #[test]
    fn coalesce_returns_a_keyless_buffer_by_move() {
        let buf: Vec<TestChange> = (0..50)
            .map(|i| TestChange {
                key: None,
                ts: i,
                value: i as i64,
            })
            .collect();
        let allocation = buf.as_ptr();
        let out = coalesce::<(), _>(buf);
        assert_eq!(out.as_ptr(), allocation, "a keyless coalesce is a move");
        assert_eq!(out.len(), 50);
    }

    #[test]
    fn coalesce_mixed_no_key_and_keyed() {
        let buf = vec![
            TestChange {
                key: None,
                ts: 1,
                value: 1,
            }, // A: kept
            TestChange {
                key: Some(1),
                ts: 2,
                value: 2,
            }, // B: collapsed
            TestChange {
                key: Some(1),
                ts: 3,
                value: 3,
            }, // C: kept
            TestChange {
                key: None,
                ts: 4,
                value: 4,
            }, // D: kept
            TestChange {
                key: Some(2),
                ts: 5,
                value: 5,
            }, // E: kept
        ];
        let out = coalesce::<(), _>(buf);
        let ts: Vec<u64> = out.iter().map(|c| c.ts).collect();
        assert_eq!(ts, vec![1, 3, 4, 5]);
    }

    #[test]
    fn frame_layout_matches_controller_decode_format() {
        let template = HeaderTemplate::new(0x010203, 1);
        let change = TestChange {
            key: None,
            ts: 0,
            value: 42,
        };
        let identity = encode_cell_identity("MarketRelay", "OhlcTasklet", "M1");
        let frame = build_cell_state_change_frame::<TestChange>(&identity, 99, &template, &change);

        let body_bytes = postcard::to_allocvec(&change).unwrap();

        // Layout: [pid_len:u16][pid][seq:u64][header:16][body_len:u32][body][crc32:u32]
        let pid_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
        assert_eq!(pid_len, identity.len());
        assert_eq!(&frame[2..2 + pid_len], identity.as_slice());

        let seq_off = 2 + pid_len;
        let header_off = seq_off + 8;
        let body_len_off = header_off + 16;
        let body_off = body_len_off + 4;

        assert_eq!(&frame[seq_off..seq_off + 8], &99u64.to_be_bytes());
        assert_eq!(frame[header_off], 1); // sdk_lang Rust = 1
        assert_eq!(
            &frame[body_len_off..body_len_off + 4],
            &(body_bytes.len() as u32).to_be_bytes(),
        );
        assert_eq!(
            &frame[body_off..body_off + body_bytes.len()],
            body_bytes.as_slice()
        );
        let crc_off = body_off + body_bytes.len();
        assert_eq!(
            &frame[crc_off..crc_off + 4],
            &crc32fast::hash(&body_bytes).to_be_bytes(),
        );
    }

    #[derive(Default)]
    struct EffTask;
    #[flezha_runner::tasklet(name = "effect_test_op")]
    impl crate::tasklet::Tasklet for EffTask {
        type Input = String;
        type Output = String;
        type Change = TestChange;
        fn run(
            &mut self,
            _: String,
            _: &mut crate::tasklet::Ctx<'_, Self>,
        ) -> Result<(), crate::tasklet::TaskletError> {
            Ok(())
        }
    }

    #[test]
    fn effect_record_frame_round_trips_and_marks_format_2() {
        let change = TestChange {
            key: None,
            ts: 7,
            value: 42,
        };
        let entry = EffectEntry::<TestChange> {
            event_time: 0,
            changes: vec![change.clone()],
            outputs: vec![OutputRecord {
                channel: "out".into(),
                key: "BTC/1m".into(),
                metadata: vec![("a".into(), "b".into())],
                payload: b"candle".to_vec(),
            }],
            consumed_offset: Some(4711),
            dlq: None,
            timers: vec![crate::cell::timer::TimerOp::register(
                crate::cell::timer::TimerKind::ProcessingTime,
                31_000,
            )],
            edges: Vec::new(),
        };
        let template = HeaderTemplate::new(0x010203, 1);
        let identity = encode_cell_identity("Task", "stage", "k");
        let frame = build_effect_record_frame(
            &identity,
            9,
            &template,
            &entry.into_body::<TypedBodies<EffTask>>(),
        );

        // Same outer layout as a change frame; header format_id = 2.
        let split = super::split_cell_frame(&frame).unwrap();
        assert_eq!(split.header[9], FORMAT_ID_EFFECT_RECORD);
        let decoded: EffectRecordBody = postcard::from_bytes(split.body).unwrap();

        assert_eq!(decoded.consumed_offset, Some(4711));
        assert_eq!(decoded.outputs.len(), 1);
        assert_eq!(decoded.outputs[0].channel, "out");
        assert_eq!(decoded.outputs[0].key, "BTC/1m");
        assert_eq!(decoded.outputs[0].payload, b"candle".to_vec());
        assert_eq!(decoded.changes.len(), 1);
        let folded: TestChange = postcard::from_bytes(&decoded.changes[0]).unwrap();
        assert_eq!(folded, change);
    }

    /// Cost-free-default guard (WAL path): a non-tier buffer drains to
    /// frames byte-identical to the pre-tier layout, under the pre-tier
    /// payload id — the tier machinery adds zero bytes and zero work.
    #[test]
    fn non_tier_drain_frames_are_byte_identical_plain_change_frames() {
        let change = TestChange {
            key: None,
            ts: 3,
            value: 9,
        };
        let mut buffer = TypedBuffer::<EffTask> {
            changes: vec![change.clone()],
            ..Default::default()
        };
        let seq = AtomicU64::new(5);
        let template = HeaderTemplate::new(0x010203, 1);
        let identity = encode_cell_identity("Task", "stage", "k");

        let (count, frames) = buffer.drain_to_frames(&identity, &seq, &template);

        assert_eq!(count, 1);
        assert_eq!(frames[0].payload_id, "cell_state_change");
        assert_eq!(
            frames[0].bytes,
            build_cell_state_change_frame::<TestChange>(&identity, 5, &template, &change)
        );
    }

    #[test]
    fn effect_records_are_never_coalesced_at_drain() {
        // Two entries whose changes share a coalesce key AND carry outputs:
        // both must ship — a coalesced-away record would drop its outputs.
        let mk = |ts: u64| EffectEntry::<TestChange> {
            event_time: ts,
            changes: vec![TestChange {
                key: Some(7),
                ts,
                value: ts as i64,
            }],
            outputs: vec![OutputRecord {
                channel: "out".into(),
                key: format!("k{ts}"),
                metadata: vec![],
                payload: vec![1],
            }],
            consumed_offset: Some(ts),
            dlq: None,
            timers: Vec::new(),
            edges: Vec::new(),
        };
        let mut buffer = TypedBuffer::<EffTask> {
            effects: vec![mk(1), mk(2)],
            ..Default::default()
        };

        let seq = AtomicU64::new(0);
        let template = HeaderTemplate::new(0x010203, 1);
        let identity = encode_cell_identity("Task", "stage", "k");
        let (count, frames) = buffer.drain_to_frames(&identity, &seq, &template);

        assert_eq!(count, 2, "output-bearing records must survive the drain");
        assert!(frames
            .iter()
            .all(|f| f.payload_id == crate::cell::state_sink::CELL_EFFECT_RECORD_PAYLOAD_ID));
        assert!(buffer.is_empty());
    }

    #[test]
    fn encode_cell_identity_round_trips_three_fields() {
        let id = encode_cell_identity("MarketRelay", "OhlcTasklet", "M1");
        // [oc_len:u8 = 11][oc][task_len:u8 = 11][task][sk_len:u16 BE = 2][sk]
        assert_eq!(id[0], 11);
        assert_eq!(&id[1..12], b"MarketRelay");
        assert_eq!(id[12], 11);
        assert_eq!(&id[13..24], b"OhlcTasklet");
        assert_eq!(&id[24..26], &2u16.to_be_bytes());
        assert_eq!(&id[26..28], b"M1");
    }
}
