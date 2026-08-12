//! The typed half of restoration: the fold that materializes `S::State`
//! from a shipped snapshot + change log. Everything erased — payload shape,
//! record iteration, header validation — lives in `flz_core::restore`.

use flz_core::header::{decode_header, FORMAT_ID_EFFECT_RECORD, FORMAT_ID_TIMER_OPS};
pub use flz_core::restore::*;
use flz_core::schema::TimerOp;
use flz_core::timer::PendingTimers;

use crate::entity::header::{
    validate_header_for_change, validate_header_for_effect_record, validate_header_for_timer_ops,
};

/// Runtime-side restoration helper. Not intended for direct user
/// invocation — the user's `Procedure::init_state` is invoked first; this
/// function applies the persisted state on top of whatever
/// `init_state` returned.
///
/// Behavior:
///   1. If `payload.snapshot_bytes` is non-empty, decode + validate the
///      header and replace `state` with `postcard::from_bytes(rest)`.
///   2. Iterate change-log records via the internal `iter_change_log_records`
///      helper; for each record, validate the header, deserialize the change
///      body, and call `S::apply_change(&mut state, &change)`.
///   3. Fold every `TimerOp` the log carries — standalone `format_id = 3`
///      records and the `timers` an effect record encloses — into the returned
///      pending-timer set. Timers are folded state, so they are rebuilt by this
///      same walk rather than by a store of their own.
pub fn init_entity_with_payload<S: crate::procedure::Procedure>(
    entity_key: Option<&str>,
    payload: Option<&RestoreInitPayload<'_>>,
) -> Result<(S::State, PendingTimers), RestoreError>
where
    S::State: serde::de::DeserializeOwned,
{
    let mut timers = PendingTimers::default();
    let mut state = S::init_state(
        entity_key,
        payload.map(|p| RestoreInitPayload {
            snapshot_bytes: p.snapshot_bytes,
            change_log_bytes: p.change_log_bytes,
        }),
    );
    if let Some(p) = payload {
        if !p.snapshot_bytes.is_empty() {
            // The controller strips the snapshot file's header before shipping;
            // the body here is the raw postcard-encoded `S::State`.
            state = postcard::from_bytes(p.snapshot_bytes)
                .map_err(|e| RestoreError::SnapshotDecode(e.to_string()))?;
        }
        for record in iter_change_log_records(p.change_log_bytes) {
            let record = record?;
            let header = decode_header(record.header)?;
            // The trailing crc32 in the WAL covers header + change_bytes.
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(record.header);
            hasher.update(record.change_bytes);
            if hasher.finalize() != record.trailing_crc {
                return Err(RestoreError::CrcMismatch {
                    entity: 0,
                    seq: record.seq,
                });
            }
            match header.format_id {
                FORMAT_ID_EFFECT_RECORD => {
                    // Effectively-once effect record: fold its change portion
                    // exactly as plain change records fold, in order. Outputs
                    // and consumed-offset are controller-side concerns here.
                    validate_header_for_effect_record::<S>(&header)?;
                    let body: crate::entity::flusher::EffectRecordBody =
                        postcard::from_bytes(record.change_bytes)
                            .map_err(|e| RestoreError::ChangeDecode(e.to_string()))?;
                    for change_bytes in &body.changes {
                        let change: S::Change = postcard::from_bytes(change_bytes)
                            .map_err(|e| RestoreError::ChangeDecode(e.to_string()))?;
                        S::apply_change(&mut state, &change);
                    }
                    for op in &body.timers {
                        timers.apply(op);
                    }
                }
                FORMAT_ID_TIMER_OPS => {
                    validate_header_for_timer_ops::<S>(&header)?;
                    let ops: Vec<TimerOp> = postcard::from_bytes(record.change_bytes)
                        .map_err(|e| RestoreError::ChangeDecode(e.to_string()))?;
                    for op in &ops {
                        timers.apply(op);
                    }
                }
                _ => {
                    validate_header_for_change::<S>(&header)?;
                    let change: S::Change = postcard::from_bytes(record.change_bytes)
                        .map_err(|e| RestoreError::ChangeDecode(e.to_string()))?;
                    S::apply_change(&mut state, &change);
                }
            }
        }
    }
    Ok((state, timers))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::flusher::{EffectRecordBody, OutputRecord};
    use crate::entity::header::{HeaderTemplate, FORMAT_ID_POSTCARD};

    #[test]
    fn empty_change_log_yields_no_records() {
        let mut iter = iter_change_log_records(&[]);
        assert!(iter.next().is_none());
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct AddChange(i64);
    impl crate::procedure::change::Change for AddChange {}

    #[derive(Default)]
    struct FoldProc;
    impl crate::procedure::Procedure for FoldProc {
        type Input = String;
        type Output = String;
        type State = i64;
        type Change = AddChange;
        type Clock = crate::procedure::WallClock;
        type Config = ();
        fn name() -> &'static str {
            "restore_fold_test_op"
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
            _: crate::procedure::state_handle::StateHandle<'_, Self>,
            _: &mut crate::procedure::ProcedureCtx<'_>,
        ) -> Result<(), crate::procedure::ProcedureError> {
            Ok(())
        }
    }

    fn wal_record(seq: u64, format_id: u8, body: &[u8]) -> Vec<u8> {
        let template = HeaderTemplate {
            format_id,
            ..crate::entity::header::header_template_for::<FoldProc>()
        };
        let header = template.stamp(body);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header);
        hasher.update(body);
        let crc = hasher.finalize();
        let mut out = Vec::new();
        out.extend_from_slice(&seq.to_be_bytes());
        out.extend_from_slice(&((16 + body.len()) as u32).to_be_bytes());
        out.extend_from_slice(&header);
        out.extend_from_slice(body);
        out.extend_from_slice(&crc.to_be_bytes());
        out
    }

    #[test]
    fn mixed_wal_folds_plain_and_effect_records_in_order() {
        // Plain change record (seq 0), then an effect record (seq 1)
        // carrying two changes + an output — tier enabled mid-lifetime.
        let plain = wal_record(
            0,
            FORMAT_ID_POSTCARD,
            &postcard::to_allocvec(&AddChange(5)).unwrap(),
        );
        let effect_body = EffectRecordBody {
            event_time: 0,
            changes: vec![
                postcard::to_allocvec(&AddChange(10)).unwrap(),
                postcard::to_allocvec(&AddChange(100)).unwrap(),
            ],
            outputs: vec![OutputRecord {
                channel: "out".into(),
                key: "k".into(),
                metadata: vec![],
                payload: vec![1, 2],
            }],
            consumed_offset: Some(4711),
            dlq: None,
            timers: Vec::new(),
        };
        let effect = wal_record(
            1,
            FORMAT_ID_EFFECT_RECORD,
            &postcard::to_allocvec(&effect_body).unwrap(),
        );

        let mut change_log = plain;
        change_log.extend_from_slice(&effect);

        let payload = RestoreInitPayload {
            snapshot_bytes: &[],
            change_log_bytes: &change_log,
        };
        let (state, _timers) = init_entity_with_payload::<FoldProc>(None, Some(&payload)).unwrap();
        assert_eq!(state, 115, "5 (plain) + 10 + 100 (effect) folded in order");
    }

    #[test]
    fn truncated_change_log_returns_header_invalid() {
        let mut iter = iter_change_log_records(&[0u8; 10]);
        let err = iter.next().unwrap().unwrap_err();
        assert!(matches!(err, RestoreError::HeaderInvalid));
    }
}
