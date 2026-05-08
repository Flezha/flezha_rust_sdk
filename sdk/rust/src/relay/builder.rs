//! `build_topology` consumes a `ChannelBuilder` (populated by
//! `Relay::channels`) and produces a frozen `Arc<RelayTopology>`.
//!
//! Build-time validation:
//! - reject duplicate channel names
//! - reject channels declared but never wired with `.from`
//! - reject multi-producer (also blocked by the type system)
//! - reject `partition_for` default impl on shards with non-empty
//!   `shard_keys()` per `design.md` Open Question 1 (resolved via the
//!   `PARTITION_FOR_OVERRIDDEN` const sentinel on the `Shard` trait)

use std::collections::HashMap;
use std::sync::Arc;

use crate::channel::builder::{BuildTopologyError, ChannelBuilder};
use crate::channel::{ChannelDescriptor, ChannelKind};
use crate::relay::topology::{RelayTopology, ShardDescriptor};

/// Built-in default per-Partition mailbox capacity used when a Shard does
/// not override `mailbox_capacity()` and `FLEZHA_MAILBOX_CAPACITY` is
/// either unset or unparseable.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

/// Hard upper bound on per-Partition mailbox capacity. ~1M items × ~40 B
/// per slot ≈ 40 MB per Partition is past sanity for any expected
/// workload; values above this fail registration with a named error.
pub const MAX_MAILBOX_CAPACITY: usize = 1_048_576;

/// Environment variable consulted when a Shard does not set
/// `MAILBOX_CAPACITY_OVERRIDDEN = true`. Parsed as `usize`; empty,
/// missing, or unparseable values fall through to
/// [`DEFAULT_MAILBOX_CAPACITY`].
pub const ENV_MAILBOX_CAPACITY: &str = "FLEZHA_MAILBOX_CAPACITY";

#[derive(Debug, thiserror::Error)]
#[error("relay topology validation failed: {errors:?}")]
pub struct TopologyBuildError {
    pub errors: Vec<BuildTopologyError>,
}

pub fn build_topology(
    factory_id: &'static str,
    builder: ChannelBuilder,
) -> Result<Arc<RelayTopology>, TopologyBuildError> {
    build_topology_with_persistence(factory_id, builder, true)
}

/// Like [`build_topology`] but accepts the `Relay::PERSISTENCE_ENABLED`
/// const captured at registration. `build_topology_for::<R>()` is the
/// canonical caller; tests that exercise topology without a Relay
/// generic call [`build_topology`] which defaults the const to `true`.
pub fn build_topology_with_persistence(
    factory_id: &'static str,
    builder: ChannelBuilder,
    persistence_enabled_const: bool,
) -> Result<Arc<RelayTopology>, TopologyBuildError> {
    let ChannelBuilder {
        channels,
        name_to_id,
        shards,
        shard_name_to_id,
        mut errors,
    } = builder;

    // Build channel descriptors and validate.
    let mut channel_descs: Vec<ChannelDescriptor> = Vec::with_capacity(channels.len());
    for entry in channels {
        if entry.producer.is_none() && entry.kind != ChannelKind::Entry {
            errors.push(BuildTopologyError::UnwiredChannel { name: entry.name });
        }
        channel_descs.push(ChannelDescriptor {
            name: Arc::from(entry.name),
            kind: entry.kind,
            producer: entry.producer,
            consumers: entry.consumers,
            plugin_name: entry.plugin_name.map(Arc::from),
            wrap_input: entry.wrap_input,
            plugin_serializer: entry.plugin_serializer,
            overflow_policy: entry.overflow_policy,
        });
    }

    // Build shard descriptors and validate partition declarations.
    let mut shard_descs: Vec<ShardDescriptor> = Vec::with_capacity(shards.len());
    for entry in shards {
        if !entry.partition_keys.is_empty() && !entry.partition_for_overridden {
            errors.push(BuildTopologyError::PartitionForMissing { name: entry.name });
        }
        if entry.shard_key_overridden && entry.partition_keys.is_empty() {
            errors.push(BuildTopologyError::DynamicModeOnly { name: entry.name });
        }
        let resolved_capacity = match resolve_mailbox_capacity(
            entry.name,
            entry.mailbox_capacity,
            entry.mailbox_capacity_overridden,
        ) {
            Ok(cap) => cap,
            Err(err) => {
                errors.push(err);
                // Stash a placeholder so the descriptor list keeps the
                // same indexing; the Err path returns before the topology
                // is published, so the placeholder is never read.
                DEFAULT_MAILBOX_CAPACITY
            }
        };
        shard_descs.push(ShardDescriptor {
            name: Arc::from(entry.name),
            partition_keys: entry.partition_keys,
            partition_for_overridden: entry.partition_for_overridden,
            shard_key_overridden: entry.shard_key_overridden,
            input_channel_id: entry.input_channel_id,
            output_channel_id: entry.output_channel_id,
            input_type_id: entry.input_type_id,
            output_type_id: entry.output_type_id,
            mailbox_capacity: resolved_capacity,
            mailbox_capacity_overridden: entry.mailbox_capacity_overridden,
            build_partition: entry.build_partition,
            partition_for_erased: entry.partition_for_erased,
            serialize_state: entry.serialize_state,
            deserialize_state: entry.deserialize_state,
        });
    }

    if !errors.is_empty() {
        return Err(TopologyBuildError { errors });
    }

    let mut name_to_id_static: HashMap<&'static str, u32> = HashMap::new();
    for (k, v) in name_to_id {
        name_to_id_static.insert(k, v);
    }
    let mut shard_name_to_id_static: HashMap<&'static str, u32> = HashMap::new();
    for (k, v) in shard_name_to_id {
        shard_name_to_id_static.insert(k, v);
    }

    Ok(Arc::new(RelayTopology {
        channels: channel_descs,
        name_to_id: name_to_id_static,
        shards: shard_descs,
        shard_name_to_id: shard_name_to_id_static,
        factory_id,
        persistence_enabled_const,
    }))
}

/// Convenience constructor used by `register_relay::<R>()`. Calls
/// `R::channels(...)` once and returns the resulting topology, capturing
/// `R::PERSISTENCE_ENABLED` so `RelayInstance::new` can resolve the
/// final per-instance flag.
pub fn build_topology_for<R: crate::relay::Relay>() -> Result<Arc<RelayTopology>, TopologyBuildError>
{
    let mut cb = ChannelBuilder::new();
    R::channels(&mut cb);
    build_topology_with_persistence(R::factory_id(), cb, R::PERSISTENCE_ENABLED)
}

/// Resolve a Shard's mailbox capacity per Decision 6: an explicit
/// override on the Shard wins over `FLEZHA_MAILBOX_CAPACITY`, which wins
/// over the built-in default. Out-of-range values fail registration with
/// the offending Shard's name; an unparseable env var falls through to
/// the default rather than failing (executors tolerate typos in env
/// values during development).
pub(crate) fn resolve_mailbox_capacity(
    shard_name: &'static str,
    declared: usize,
    overridden: bool,
) -> Result<usize, BuildTopologyError> {
    let env_value = std::env::var(ENV_MAILBOX_CAPACITY).ok();
    resolve_mailbox_capacity_inner(shard_name, declared, overridden, env_value.as_deref())
}

/// Pure-function core split out of [`resolve_mailbox_capacity`] so unit
/// tests can exercise the precedence rules without mutating the
/// process-global `FLEZHA_MAILBOX_CAPACITY` env var.
pub(crate) fn resolve_mailbox_capacity_inner(
    shard_name: &'static str,
    declared: usize,
    overridden: bool,
    env_value: Option<&str>,
) -> Result<usize, BuildTopologyError> {
    let resolved = if overridden {
        declared
    } else {
        env_value
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAILBOX_CAPACITY)
    };
    validate_capacity(shard_name, resolved)
}

fn validate_capacity(
    shard_name: &'static str,
    value: usize,
) -> Result<usize, BuildTopologyError> {
    if value == 0 {
        return Err(BuildTopologyError::MailboxCapacityZero { shard: shard_name });
    }
    if value > MAX_MAILBOX_CAPACITY {
        return Err(BuildTopologyError::MailboxCapacityTooLarge {
            shard: shard_name,
            value,
        });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn override_on_shard_takes_precedence() {
        // Shard declared 4096 with MAILBOX_CAPACITY_OVERRIDDEN = true;
        // env var present and parseable. Override wins.
        let cap = resolve_mailbox_capacity_inner("ohlc", 4096, true, Some("2048")).unwrap();
        assert_eq!(cap, 4096);
    }

    #[test]
    fn env_var_applies_when_no_shard_override() {
        // No override; env var is parseable. Env wins over the default.
        let cap = resolve_mailbox_capacity_inner("ohlc", 1024, false, Some("2048")).unwrap();
        assert_eq!(cap, 2048);
    }

    #[test]
    fn env_var_unparseable_falls_through_to_default() {
        // No override; env var present but unparseable. Default kicks in.
        let cap = resolve_mailbox_capacity_inner("ohlc", 1024, false, Some("banana")).unwrap();
        assert_eq!(cap, DEFAULT_MAILBOX_CAPACITY);
    }

    #[test]
    fn env_var_missing_falls_through_to_default() {
        let cap = resolve_mailbox_capacity_inner("ohlc", 1024, false, None).unwrap();
        assert_eq!(cap, DEFAULT_MAILBOX_CAPACITY);
    }

    #[test]
    fn capacity_zero_is_rejected_with_shard_name() {
        // Override path: explicit 0 is rejected.
        let err = resolve_mailbox_capacity_inner("ohlc", 0, true, None).unwrap_err();
        match err {
            BuildTopologyError::MailboxCapacityZero { shard } => assert_eq!(shard, "ohlc"),
            other => panic!("expected MailboxCapacityZero, got {other:?}"),
        }
    }

    #[test]
    fn capacity_above_maximum_is_rejected_with_shard_name() {
        let err = resolve_mailbox_capacity_inner(
            "ohlc",
            MAX_MAILBOX_CAPACITY + 1,
            true,
            None,
        )
        .unwrap_err();
        match err {
            BuildTopologyError::MailboxCapacityTooLarge { shard, value } => {
                assert_eq!(shard, "ohlc");
                assert_eq!(value, MAX_MAILBOX_CAPACITY + 1);
            }
            other => panic!("expected MailboxCapacityTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn override_zero_overrides_env_var_and_fails() {
        // Even if env is valid, an explicit-but-invalid override fails
        // (override-flag-true wins precedence even into validation).
        let err =
            resolve_mailbox_capacity_inner("ohlc", 0, true, Some("4096")).unwrap_err();
        assert!(matches!(
            err,
            BuildTopologyError::MailboxCapacityZero { shard: "ohlc" }
        ));
    }

    #[test]
    fn capacity_one_is_accepted() {
        let cap = resolve_mailbox_capacity_inner("tiny", 1, true, None).unwrap();
        assert_eq!(cap, 1);
    }

    #[test]
    fn maximum_capacity_is_accepted() {
        let cap = resolve_mailbox_capacity_inner(
            "max_shard",
            MAX_MAILBOX_CAPACITY,
            true,
            None,
        )
        .unwrap();
        assert_eq!(cap, MAX_MAILBOX_CAPACITY);
    }
}
