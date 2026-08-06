use prost::Message;

use super::CommandAction;
use crate::bounded::spec::BoundedSpec;
use crate::pb::{AddOperative, AddOperativeResponse};
use crate::runner::RunnerNode;

impl CommandAction for AddOperative {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let instance_id = self.operative_id.clone();
        let factory_id = self.factory_id.clone();

        // Empty kind ⇒ `unbounded`, the wire default.
        let kind = if self.kind.is_empty() {
            "unbounded"
        } else {
            self.kind.as_str()
        };

        // Idempotent on reconnect: the controller re-issues AddOperative for
        // every UnboundedOperative instance ProcessHub still has tagged to this
        // runner's profile after a restart. If the instance is already
        // in the local registry, skip without re-registering — keeps
        // the existing per-Entity state intact and prevents
        // duplicate-init crashes.
        if runner.has_operative(&instance_id) {
            return (
                "start_operative".to_string(),
                metadata_response(kind, &factory_id),
            );
        }

        match kind {
            "bounded" => {
                if !runner.start_bounded_resumable(
                    factory_id.clone(),
                    instance_id.clone(),
                    self.init_values.clone(),
                    self.resume_entity_count,
                    self.proc_journal.as_ref().map(Into::into),
                ) {
                    errors.push(format!(
                        "BoundedOperative class with factory_id {} is not registered",
                        factory_id
                    ));
                }
            }
            "unbounded" => match runner.start_operative(factory_id.clone(), instance_id.clone()) {
                Some(instance) => {
                    if !self.init_values.is_empty() {
                        if let Err(e) = crate::command::entity_state::apply_init_values(
                            &instance,
                            &self.init_values,
                        ) {
                            errors.push(e);
                        }
                    }
                }
                None => {
                    errors.push(format!(
                        "UnboundedOperative class with factory_id {} is not registered",
                        factory_id
                    ));
                }
            },
            other => {
                errors.push(format!(
                    "unknown compute-unit kind '{}' for unit {}",
                    other, instance_id
                ));
            }
        }

        (
            "start_operative".to_string(),
            metadata_response(kind, &factory_id),
        )
    }
}

/// Encode the class's SDK-side declarations as `AddOperativeResponse`
/// metadata for the controller's capability resolution. Returns empty
/// bytes when nothing is declared (the default), keeping the response
/// byte-identical to pre-tier builds.
fn metadata_response(kind: &str, factory_id: &str) -> Vec<u8> {
    use crate::operative::topology::{AckGranularity, BeyondWindowPolicy};

    let mut metadata = std::collections::HashMap::new();

    // Every unbounded-only declaration reads the same topology; resolve it once.
    // `None` for a bounded class, or one that never registered.
    let unbounded = match kind {
        "bounded" => None,
        _ => crate::operative::registry::get_class(factory_id),
    };

    let effectively_once = match kind {
        "bounded" => crate::bounded::registry::get_class(factory_id)
            .and_then(|spec| spec.topology.as_ref().map(|t| t.effectively_once))
            .unwrap_or(false),
        _ => unbounded.as_ref().is_some_and(|t| t.effectively_once),
    };
    if effectively_once {
        metadata.insert("effectively_once".to_string(), "true".to_string());
    }

    if let Some(topology) = &unbounded {
        // Late-input correction. The controller owns the two prerequisites the
        // SDK cannot see — the input-log hold window and the snapshot age-floor
        // must both cover this window, or the past it promises to repair is
        // already gone — so it needs the window before it binds the operative.
        if let Some(policy) = topology.correction {
            metadata.insert(
                "correction_window_ms".to_string(),
                policy.window_ms.to_string(),
            );
            metadata.insert(
                "beyond_window_policy".to_string(),
                match policy.beyond_window {
                    BeyondWindowPolicy::ProcessAsArrival => "process_as_arrival",
                    BeyondWindowPolicy::Drop => "drop",
                }
                .to_string(),
            );
        }

        // Checkpoint ack-granularity. An Entity that only acknowledges at a
        // checkpoint holds its input-log watermark still for the whole interval
        // — the controller's silence-based liveness detectors have to read that
        // as the steady state it is, not as a stall.
        if topology.ack_granularity == AckGranularity::Checkpoint {
            metadata.insert("ack_granularity".to_string(), "checkpoint".to_string());
        }

        // The per-Entity WAL size compaction trigger. The controller resolves
        // it per operative_class, so ship the most conservative (smallest)
        // declared threshold across this operative's procedures — only when it
        // differs from the shared default, keeping an unchanged class's reply
        // byte-identical.
        if let Some(min_bytes) = topology
            .procedures
            .iter()
            .map(|proc| proc.compact_after_bytes)
            .min()
        {
            if min_bytes != crate::procedure::DEFAULT_COMPACT_AFTER_BYTES {
                metadata.insert("compact_after_bytes".to_string(), min_bytes.to_string());
            }
        }
    }

    // The tree positions whose procedure declares `compensate`. A rewind can
    // only unwind a procedure whose plugin output is semantically undoable, and
    // the controller refuses one that is not — so it needs this before it wakes
    // the instance (bounded-loop-rewind D5).
    if let Some(procs) = declared_procs(kind, factory_id, |spec, node| {
        spec.topology
            .as_ref()
            .and_then(|t| t.procedure(spec.dag.order[node]))
            .is_some_and(|proc| proc.compensate_overridden)
    }) {
        metadata.insert("compensable_procs".to_string(), procs);
    }

    // The tree's leaf positions. The operative is DONE when all of them have
    // committed, and the controller derives that from the journal alone —
    // which needs the leaf set (bounded-branching D4).
    if let Some(procs) = declared_procs(kind, factory_id, |spec, node| {
        spec.dag.children[node].is_empty()
    }) {
        metadata.insert("leaf_procs".to_string(), procs);
    }

    if metadata.is_empty() {
        return Vec::new();
    }
    AddOperativeResponse { metadata }.encode_to_vec()
}

/// Comma-separated tree positions of the procedures `select` picks out, or
/// `None` for anything without a structured tree (run-sugar, unbounded) — the
/// one seam every bounded procedure-set declaration rides. An empty string is
/// meaningful — "a tree, none of it selected" — and is not the same as absent.
fn declared_procs(
    kind: &str,
    factory_id: &str,
    select: impl Fn(&BoundedSpec, usize) -> bool,
) -> Option<String> {
    if kind != "bounded" {
        return None;
    }
    let spec = crate::bounded::registry::get_class(factory_id)?;
    spec.topology.as_ref()?;
    Some(
        (0..spec.dag.len())
            .filter(|&node| select(&spec, node))
            .map(|node| node.to_string())
            .collect::<Vec<_>>()
            .join(","),
    )
}
