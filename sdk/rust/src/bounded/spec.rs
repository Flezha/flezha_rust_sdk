//! Bounded class spec: the frozen authoring result produced at registration.
//!
//! A bounded operative is authored either as bare [`run`](super::BoundedOperative::run)
//! sugar (no declared operations) or as a structured **linear chain** of
//! Operations/Channels/Segments via
//! [`operations`](super::BoundedOperative::operations). `build_spec` runs the
//! declaration once, validates it (linear chain, no dynamic segments,
//! self-terminating), and freezes the result for every spawn of the class.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::channel::builder::ChannelBuilder;
use crate::channel::{ChannelKind, OperationId};
use crate::operative::builder::build_topology;
use crate::operative::topology::{OperativeTopology, SegmentingMode};

use super::{BoundedOperative, BoundedRunFn};

/// Frozen per-class authoring result stored in the registry. `topology` is
/// `None` for the run-sugar form; `Some` (with the ordered `chain`) for the
/// structured form.
pub(crate) struct BoundedSpec {
    /// Run-sugar body. Invoked only when `topology` is `None`.
    pub run: BoundedRunFn,
    /// Structured topology, or `None` for run-sugar.
    pub topology: Option<Arc<OperativeTopology>>,
    /// Operations in execution order (entry consumer → … → terminal). Empty
    /// for run-sugar.
    pub chain: Vec<OperationId>,
    /// Optional wall-clock backstop applied by the driver.
    pub max_duration: Option<Duration>,
}

// Manual: `OperativeTopology` holds closures and is not `Debug`. A summary is
// enough for test assertions (`expect`/`expect_err`) and diagnostics.
impl std::fmt::Debug for BoundedSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedSpec")
            .field("structured", &self.topology.is_some())
            .field("chain_len", &self.chain.len())
            .field("max_duration", &self.max_duration)
            .finish()
    }
}

/// Registration-time rejections for a bounded operative authoring.
#[derive(Debug, thiserror::Error)]
pub enum BuildBoundedError {
    #[error("bounded operative `{factory_id}` has an invalid topology: {detail}")]
    Topology {
        factory_id: &'static str,
        detail: String,
    },

    #[error(
        "bounded operative `{factory_id}` declares operations but no entry channel; a structured \
         bounded must declare one input channel (c.input::<T>(...)) to seed its first operation"
    )]
    NoEntryChannel { factory_id: &'static str },

    #[error(
        "bounded operative `{factory_id}` declares a branching topology at `{at}`; bounded v1 \
         supports a single linear chain only (each stage feeds exactly one downstream operation)"
    )]
    Branching { factory_id: &'static str, at: String },

    #[error(
        "bounded operative `{factory_id}` declares a dynamic-segment operation `{operation}`; \
         bounded operatives do not support dynamic segmentation"
    )]
    DynamicSegments {
        factory_id: &'static str,
        operation: String,
    },

    #[error(
        "bounded operative `{factory_id}` is not terminable: its only possible exit is an external \
         stop. Add a terminal condition — let run()/the chain complete, give the loop a terminal \
         predicate, or declare max_duration()."
    )]
    ExternalStopOnly { factory_id: &'static str },

    #[error(
        "bounded operative `{factory_id}` declares operation `{operation}` that is not reachable \
         from the entry channel; bounded v1 requires one connected linear chain"
    )]
    Disconnected {
        factory_id: &'static str,
        operation: String,
    },
}

/// Build and validate the spec for a bounded class. Empty `operations()`
/// selects the run-sugar path; a non-empty declaration is validated as a
/// linear chain.
pub(crate) fn build_spec<T: BoundedOperative>() -> Result<BoundedSpec, BuildBoundedError> {
    let factory_id = T::factory_id();

    // Self-termination guard: an operative whose only exit is an external stop,
    // with no duration backstop, can never terminate cooperatively.
    if !T::self_terminating() && T::max_duration().is_none() {
        return Err(BuildBoundedError::ExternalStopOnly { factory_id });
    }

    let mut cb = ChannelBuilder::new();
    T::operations(&mut cb);

    // No declared operations ⇒ run-sugar.
    if cb.operations.is_empty() {
        return Ok(BoundedSpec {
            run: T::run as BoundedRunFn,
            topology: None,
            chain: Vec::new(),
            max_duration: T::max_duration(),
        });
    }

    let topology = build_topology(factory_id, cb).map_err(|e| BuildBoundedError::Topology {
        factory_id,
        detail: format!("{e}"),
    })?;
    let chain = linear_chain(factory_id, &topology)?;

    Ok(BoundedSpec {
        run: T::run as BoundedRunFn,
        topology: Some(topology),
        chain,
        max_duration: T::max_duration(),
    })
}

/// Resolve the linear execution order of a structured bounded's operations
/// and reject anything that is not a single connected chain (branching,
/// dynamic segmentation, cycles, disconnected operations, no entry channel).
fn linear_chain(
    factory_id: &'static str,
    topology: &OperativeTopology,
) -> Result<Vec<OperationId>, BuildBoundedError> {
    // Bounded has no `dynamic_segments?` capability.
    for op in &topology.operations {
        if op.mode == SegmentingMode::Dynamic {
            return Err(BuildBoundedError::DynamicSegments {
                factory_id,
                operation: op.name.to_string(),
            });
        }
    }

    // The entry channel seeds the first operation from the spawn args.
    let entry = topology
        .channels
        .iter()
        .find(|c| c.kind == ChannelKind::Entry)
        .ok_or(BuildBoundedError::NoEntryChannel { factory_id })?;
    if entry.consumers.len() != 1 {
        return Err(BuildBoundedError::Branching {
            factory_id,
            at: format!("entry channel `{}`", entry.name),
        });
    }

    let mut chain: Vec<OperationId> = Vec::with_capacity(topology.operations.len());
    let mut seen: HashSet<OperationId> = HashSet::new();
    let mut current = entry.consumers[0];

    loop {
        if !seen.insert(current) {
            // Re-visiting an operation ⇒ a cycle: not a linear chain.
            let name = topology
                .operation(current)
                .map(|o| o.name.to_string())
                .unwrap_or_default();
            return Err(BuildBoundedError::Branching {
                factory_id,
                at: format!("operation `{name}` (cycle)"),
            });
        }
        chain.push(current);

        let op = topology
            .operation(current)
            .expect("chain operation present in topology");
        let next = match op.output_channel_id.and_then(|cid| topology.channel(cid)) {
            Some(ch) => {
                if ch.consumers.len() > 1 {
                    return Err(BuildBoundedError::Branching {
                        factory_id,
                        at: format!("operation `{}`", op.name),
                    });
                }
                ch.consumers.first().copied()
            }
            None => None,
        };

        match next {
            Some(n) => current = n,
            None => break,
        }
    }

    if chain.len() != topology.operations.len() {
        let disconnected = topology
            .operations
            .iter()
            .enumerate()
            .find(|(i, _)| !seen.contains(&(*i as OperationId)))
            .map(|(_, op)| op.name.to_string())
            .unwrap_or_default();
        return Err(BuildBoundedError::Disconnected {
            factory_id,
            operation: disconnected,
        });
    }

    Ok(chain)
}
