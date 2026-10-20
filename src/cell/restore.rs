//! The typed half of restoration: the snapshot decode, and the output that
//! folds change bodies into `S::State`. Everything erased — payload shape,
//! record framing, header resolution — lives in `flz_core::restore`.

use flz_core::header::SchemaDecision;
pub use flz_core::restore::*;
use flz_core::schema::TimerOp;
use flz_core::timer::PendingTimers;

use crate::cell::header::header_template_for;
use crate::tasklet::Change;

/// Runtime-side restoration helper. Not intended for direct user
/// invocation — the user's `Tasklet::init_state` is invoked first; this
/// function applies the persisted state on top of whatever
/// `init_state` returned.
///
/// The walk is `flz_core::restore::fold_restore`: a non-empty snapshot is
/// decoded (or handed to `S::migrate_state`) and replaces the state, then
/// every change body is decoded (or migrated) and applied in seq order, and
/// every `TimerOp` the log carries folds into the returned pending set. The
/// version decision is per record, because one WAL legitimately holds
/// records written by more than one version of the Tasklet. The outcome
/// carries the terminal seq and whether anything was read through a
/// migration; the controller answers that with a compaction so the
/// migration is paid once.
pub fn init_cell_with_payload<S: crate::tasklet::Tasklet>(
    cell_key: Option<&str>,
    payload: Option<&RestoreInitPayload<'_>>,
) -> Result<(S::State, PendingTimers, RestoreOutcome), RestoreError> {
    let mut timers = PendingTimers::default();
    let mut state = S::init_state(cell_key);
    let outcome = match payload {
        Some(p) => fold_restore(
            p,
            &header_template_for::<S>(),
            S::MIN_READABLE_SCHEMA_VERSION,
            &mut TypedSink::<S> {
                state: &mut state,
                timers: &mut timers,
            },
        )?,
        None => RestoreOutcome::default(),
    };
    Ok((state, timers, outcome))
}

/// One change record's body read back as `S::Change`: decoded as this
/// version wrote it, or handed to `S::migrate_change` for an older one.
pub(crate) fn decode_change<S: crate::tasklet::Tasklet>(
    decision: SchemaDecision,
    bytes: &[u8],
) -> Result<S::Change, RestoreError> {
    match decision {
        SchemaDecision::Decode => {
            postcard::from_bytes(bytes).map_err(|e| RestoreError::ChangeDecode(e.to_string()))
        }
        SchemaDecision::Migrate { from_version } => {
            S::migrate_change(from_version, bytes).map_err(|reason| RestoreError::MigrationFailed {
                from_version,
                reason,
            })
        }
    }
}

/// The in-process output: the snapshot body replaces the state, and each
/// change body is decoded (or migrated) into the Tasklet's `Change` and folded
/// straight into it.
struct TypedSink<'a, S: crate::tasklet::Tasklet> {
    state: &'a mut S::State,
    timers: &'a mut PendingTimers,
}

impl<S: crate::tasklet::Tasklet> ChangeLogSink for TypedSink<'_, S> {
    fn snapshot(&mut self, decision: SchemaDecision, bytes: &[u8]) -> Result<(), RestoreError> {
        *self.state = match decision {
            SchemaDecision::Decode => postcard::from_bytes(bytes)
                .map_err(|e| RestoreError::SnapshotDecode(e.to_string()))?,
            SchemaDecision::Migrate { from_version } => S::migrate_state(from_version, bytes)
                .map_err(|reason| RestoreError::MigrationFailed {
                    from_version,
                    reason,
                })?,
        };
        Ok(())
    }

    fn change(&mut self, decision: SchemaDecision, bytes: &[u8]) -> Result<(), RestoreError> {
        decode_change::<S>(decision, bytes)?.apply_to(self.state);
        Ok(())
    }

    fn timers(&mut self, ops: &[TimerOp]) {
        for op in ops {
            self.timers.apply(op);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell::flusher::{EffectRecordBody, OutputRecord};
    use crate::cell::header::{HeaderTemplate, FORMAT_ID_EFFECT_RECORD, FORMAT_ID_POSTCARD};

    #[test]
    fn empty_change_log_yields_no_records() {
        let mut iter = iter_change_log_records(&[]);
        assert!(iter.next().is_none());
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct AddChange(i64);
    impl crate::tasklet::change::Change<i64> for AddChange {
        fn apply_to(&self, state: &mut i64) {
            *state += self.0;
        }
    }

    #[derive(Default)]
    struct FoldTask;
    #[flezha_runner::tasklet(name = "restore_fold_test_op")]
    impl crate::tasklet::Tasklet for FoldTask {
        type Input = String;
        type Output = String;
        type State = i64;
        type Change = AddChange;
        fn run(
            &mut self,
            _: String,
            _: &mut crate::tasklet::Ctx<'_, Self>,
        ) -> Result<(), crate::tasklet::TaskletError> {
            Ok(())
        }
    }

    fn wal_record(seq: u64, format_id: u8, body: &[u8]) -> Vec<u8> {
        wal_record_at(
            seq,
            format_id,
            <FoldTask as crate::tasklet::Tasklet>::SCHEMA_VERSION,
            body,
        )
    }

    /// One WAL record stamped at an arbitrary schema version — what a binary
    /// deployed before a bump left behind.
    fn wal_record_at(seq: u64, format_id: u8, schema_version: u32, body: &[u8]) -> Vec<u8> {
        let template = HeaderTemplate {
            format_id,
            schema_version,
            ..crate::cell::header::header_template_for::<FoldTask>()
        };
        flz_core::restore::frame_record(seq, &template.stamp(body), body)
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
            edges: Vec::new(),
        };
        let effect = wal_record(
            1,
            FORMAT_ID_EFFECT_RECORD,
            &postcard::to_allocvec(&effect_body).unwrap(),
        );

        let mut change_log = plain;
        change_log.extend_from_slice(&effect);

        let payload = RestoreInitPayload {
            snapshot_header: &[],
            snapshot_bytes: &[],
            change_log_bytes: &change_log,
        };
        let (state, _timers, _migrated) =
            init_cell_with_payload::<FoldTask>(None, Some(&payload)).unwrap();
        assert_eq!(state, 115, "5 (plain) + 10 + 100 (effect) folded in order");
    }

    // ---- schema evolution ------------------------------------------------
    //
    // The shape of a real deploy: v1 is in production holding state, and the
    // author edits the very types the state is made of.

    #[derive(serde::Serialize, serde::Deserialize, Debug, Default, PartialEq)]
    struct CartV1 {
        items: u32,
        coupons: u32,
    }

    /// v2 with a field added — the edit every other system calls safe.
    #[derive(serde::Serialize, serde::Deserialize, Debug, Default, PartialEq)]
    struct CartV2 {
        items: u32,
        coupons: u32,
        currency: String,
    }

    /// v2 with the same two fields declared in the other order. Postcard is
    /// positional, so v1 bytes "decode" into this — transposed and silent.
    #[derive(serde::Serialize, serde::Deserialize, Debug, Default, PartialEq)]
    struct CartV2Reordered {
        coupons: u32,
        items: u32,
    }

    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    struct CartChangeV1(u32);
    impl crate::tasklet::change::Change<CartV1> for CartChangeV1 {
        fn apply_to(&self, state: &mut CartV1) {
            state.items += self.0;
        }
    }
    impl crate::tasklet::change::Change<CartV2Reordered> for CartChangeV1 {
        fn apply_to(&self, state: &mut CartV2Reordered) {
            state.items += self.0;
        }
    }

    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    struct CartChangeV2 {
        items: u32,
        note: String,
    }
    impl crate::tasklet::change::Change<CartV2> for CartChangeV2 {
        fn apply_to(&self, state: &mut CartV2) {
            state.items += self.items;
        }
    }

    /// One Tasklet per "deploy": the differences that matter — the state
    /// type, the declared version, the readable floor, the migrations — stay
    /// visible on one line each.
    macro_rules! cart_tasklet {
        ($task:ident, $state:ty, $change:ty, $v:expr, $min:expr, { $($hooks:tt)* }) => {
            #[derive(Default)]
            struct $task;
            #[flezha_runner::tasklet(
                name = "cart_tasklet",
                schema_version = $v,
                min_readable_schema_version = $min
            )]
            impl crate::tasklet::Tasklet for $task {
                type Input = String;
                type Output = String;
                type State = $state;
                type Change = $change;
                fn run(
                    &mut self,
                    _: String,
                    _: &mut crate::tasklet::Ctx<'_, Self>,
                ) -> Result<(), crate::tasklet::TaskletError> {
                    Ok(())
                }
                $($hooks)*
            }
        };
    }

    cart_tasklet!(CartV1Task, CartV1, CartChangeV1, 1, 1, {});

    // The author did the work: bumped, declared the floor, wrote both
    // migrations.
    cart_tasklet!(CartV2Task, CartV2, CartChangeV2, 2, 1, {
        fn migrate_state(from_version: u32, bytes: &[u8]) -> Result<CartV2, String> {
            match from_version {
                1 => {
                    let old: CartV1 = postcard::from_bytes(bytes).map_err(|e| e.to_string())?;
                    Ok(CartV2 {
                        items: old.items,
                        coupons: old.coupons,
                        currency: "EUR".to_string(),
                    })
                }
                v => Err(format!("no migration from {v}")),
            }
        }
        fn migrate_change(from_version: u32, bytes: &[u8]) -> Result<CartChangeV2, String> {
            match from_version {
                1 => {
                    let old: CartChangeV1 =
                        postcard::from_bytes(bytes).map_err(|e| e.to_string())?;
                    Ok(CartChangeV2 {
                        items: old.0,
                        note: String::new(),
                    })
                }
                v => Err(format!("no migration from {v}")),
            }
        }
    });

    // The author bumped and stopped there: the default floor reads only what
    // this version wrote.
    cart_tasklet!(CartV2ReorderedTask, CartV2Reordered, CartChangeV1, 2, 2, {});

    /// A v1 snapshot as the controller stores and now ships it: body plus the
    /// 16-byte header it was written with.
    fn v1_snapshot() -> (Vec<u8>, Vec<u8>) {
        let body = postcard::to_allocvec(&CartV1 {
            items: 7,
            coupons: 2,
        })
        .unwrap();
        let header = crate::cell::header::header_template_for::<CartV1Task>().stamp(&body);
        (header.to_vec(), body)
    }

    /// fails when: the snapshot is decoded without resolving its header —
    /// v1 bytes then "restore" into the reordered v2 struct with `items` and
    /// `coupons` transposed, `Ok`, and nothing reported.
    #[test]
    fn a_reordered_state_is_refused_rather_than_transposed() {
        let (header, body) = v1_snapshot();
        let payload = RestoreInitPayload {
            snapshot_header: &header,
            snapshot_bytes: &body,
            change_log_bytes: &[],
        };

        let err = init_cell_with_payload::<CartV2ReorderedTask>(None, Some(&payload))
            .expect_err("a snapshot from another schema version must not decode");
        assert!(matches!(
            err,
            RestoreError::SchemaTooOld {
                found: 1,
                min_readable: 2
            }
        ));
    }

    /// fails when: an added field has no way back — the case that reads as
    /// `Hit the end of buffer` with no seam to write a conversion in.
    #[test]
    fn an_added_field_restores_through_a_declared_migration() {
        let (header, body) = v1_snapshot();
        let payload = RestoreInitPayload {
            snapshot_header: &header,
            snapshot_bytes: &body,
            change_log_bytes: &[],
        };

        let (state, _timers, outcome) =
            init_cell_with_payload::<CartV2Task>(None, Some(&payload)).expect("migrated restore");

        assert_eq!(
            state,
            CartV2 {
                items: 7,
                coupons: 2,
                currency: "EUR".to_string()
            }
        );
        assert!(
            outcome.migrated,
            "the controller compacts on the strength of this"
        );
    }

    /// fails when: the version decision is made once per restore instead of
    /// once per record. A deploy leaves exactly this WAL behind: records
    /// written before it sit under records written after it.
    #[test]
    fn a_mixed_version_wal_folds_in_seq_order() {
        let (header, body) = v1_snapshot();
        let mut change_log = wal_record_at(
            0,
            FORMAT_ID_POSTCARD,
            1,
            &postcard::to_allocvec(&CartChangeV1(5)).unwrap(),
        );
        change_log.extend_from_slice(&wal_record_at(
            1,
            FORMAT_ID_POSTCARD,
            2,
            &postcard::to_allocvec(&CartChangeV2 {
                items: 10,
                note: "post-deploy".to_string(),
            })
            .unwrap(),
        ));
        let payload = RestoreInitPayload {
            snapshot_header: &header,
            snapshot_bytes: &body,
            change_log_bytes: &change_log,
        };

        let (state, _timers, outcome) =
            init_cell_with_payload::<CartV2Task>(None, Some(&payload)).expect("mixed-version fold");

        assert_eq!(state.items, 22, "7 (snapshot) + 5 (v1) + 10 (v2)");
        assert!(outcome.migrated);
    }

    /// fails when: the migration is wired into the plain-record arm and
    /// forgotten in the effect-record arm, so an effectively-once class loses
    /// its history on the version it migrated everywhere else.
    #[test]
    fn an_effect_records_change_bodies_migrate_too() {
        let effect = EffectRecordBody {
            event_time: 0,
            changes: vec![postcard::to_allocvec(&CartChangeV1(4)).unwrap()],
            outputs: vec![],
            consumed_offset: Some(1),
            dlq: None,
            timers: Vec::new(),
            edges: Vec::new(),
        };
        let change_log = wal_record_at(
            0,
            FORMAT_ID_EFFECT_RECORD,
            1,
            &postcard::to_allocvec(&effect).unwrap(),
        );
        let payload = RestoreInitPayload {
            snapshot_header: &[],
            snapshot_bytes: &[],
            change_log_bytes: &change_log,
        };

        let (state, _timers, outcome) =
            init_cell_with_payload::<CartV2Task>(None, Some(&payload)).expect("effect fold");

        assert_eq!(state.items, 4);
        assert!(outcome.migrated);
    }
}
