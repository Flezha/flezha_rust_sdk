//! Section 2 unit tests: topology construction + RelayInstance partition
//! creation. Covers the umbrella scenarios:
//! - "Single partition for a Shard with no partitioning declared"
//! - "Static partitions created at relay startup"
//! - "Dynamic-mode-only declaration is rejected in v1"
//! - "Entry channel of arbitrary type fails at startup" (compile-time
//!   covered by tests/ui/entry_channel_disallowed_type.rs)

use std::sync::Arc;

use flezha_executor::channel::ChannelBuilder;
use flezha_executor::relay::builder::build_topology_for;
use flezha_executor::shard::{Shard, ShardCtx, ShardError};
use flezha_executor::state::{Change, NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_executor::Relay;

#[derive(Default)]
struct EchoShard;
impl Shard for EchoShard {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "echo_shard"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(&mut self, _: String, _: StateHandle<'_, Self>, _: &mut ShardCtx<'_>) -> Result<(), ShardError> {
        Ok(())
    }
}

struct SingleRelay;
impl Relay for SingleRelay {
    fn factory_id() -> &'static str {
        "single_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("inp").into::<EchoShard>();
    }
}

#[test]
fn single_mode_partition_creation() {
    let topology = build_topology_for::<SingleRelay>().expect("build topology");
    assert_eq!(topology.shards.len(), 1);
    assert_eq!(topology.channels.len(), 1);
    assert!(topology.shards[0].partition_keys.is_empty());
    assert_eq!(topology.shards[0].input_channel_id, Some(0));
    assert_eq!(topology.channels[0].consumers, vec![0]);
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
struct AppendChange(String);
impl Change for AppendChange {}

#[derive(Default)]
struct StaticShard;
impl Shard for StaticShard {
    type Input = String;
    type Output = String;
    type State = Vec<String>;
    type Change = AppendChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "static_shard"
    }
    fn init_state(shard_key: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> Vec<String> {
        vec![shard_key.unwrap_or("none").to_string()]
    }
    fn apply_change(state: &mut Vec<String>, change: &AppendChange) {
        state.push(change.0.clone());
    }
    fn shard_keys() -> Vec<&'static str> {
        vec!["a", "b", "c"]
    }
    const PARTITION_FOR_OVERRIDDEN: bool = true;
    fn partition_for(input: &String) -> Vec<&'static str> {
        if input.starts_with('a') {
            vec!["a"]
        } else if input.starts_with('b') {
            vec!["b"]
        } else {
            vec!["c"]
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        state.apply(AppendChange(input));
        Ok(())
    }
}

struct StaticRelay;
impl Relay for StaticRelay {
    fn factory_id() -> &'static str {
        "static_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("inp").into::<StaticShard>();
    }
}

#[test]
fn static_mode_partition_creation() {
    let topology = build_topology_for::<StaticRelay>().expect("build topology");
    assert_eq!(topology.shards.len(), 1);
    assert_eq!(topology.shards[0].partition_keys, vec!["a", "b", "c"]);
    assert!(topology.shards[0].partition_for_overridden);
}

/// Dynamic-mode-only Shard: overrides `shard_key` without declaring
/// `shard_keys`. v1 rejects this at build_topology time.
#[derive(Default)]
struct DynamicOnlyShard;
impl Shard for DynamicOnlyShard {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "dynamic_only_shard"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    const SHARD_KEY_OVERRIDDEN: bool = true;
    fn shard_key(_input: &String) -> Option<String> {
        Some("k".to_string())
    }
    fn run(&mut self, _: String, _: StateHandle<'_, Self>, _: &mut ShardCtx<'_>) -> Result<(), ShardError> {
        Ok(())
    }
}

struct DynamicOnlyRelay;
impl Relay for DynamicOnlyRelay {
    fn factory_id() -> &'static str {
        "dynamic_only_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("inp").into::<DynamicOnlyShard>();
    }
}

#[test]
fn dynamic_mode_only_rejection() {
    let err = build_topology_for::<DynamicOnlyRelay>()
        .err()
        .expect("expected dynamic-mode-only rejection");
    let msg = format!("{:?}", err.errors);
    assert!(
        msg.contains("DynamicModeOnly"),
        "expected DynamicModeOnly error, got {msg}"
    );
}

/// Static-mode shard whose `partition_for` override is missing the
/// matching `PARTITION_FOR_OVERRIDDEN = true` const.
#[derive(Default)]
struct UnannotatedStaticShard;
impl Shard for UnannotatedStaticShard {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "unannotated_static_shard"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn shard_keys() -> Vec<&'static str> {
        vec!["a"]
    }
    fn partition_for(_: &String) -> Vec<&'static str> {
        vec!["a"]
    }
    fn run(&mut self, _: String, _: StateHandle<'_, Self>, _: &mut ShardCtx<'_>) -> Result<(), ShardError> {
        Ok(())
    }
}

struct UnannotatedRelay;
impl Relay for UnannotatedRelay {
    fn factory_id() -> &'static str {
        "unannotated_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("inp").into::<UnannotatedStaticShard>();
    }
}

#[test]
fn unannotated_static_mode_rejection() {
    let err = build_topology_for::<UnannotatedRelay>()
        .err()
        .expect("expected partition_for-missing rejection");
    let msg = format!("{:?}", err.errors);
    assert!(
        msg.contains("PartitionForMissing"),
        "expected PartitionForMissing error, got {msg}"
    );
}

/// Shard whose `mailbox_capacity()` returns 0; `build_topology` must
/// reject the relay with the offending Shard's name.
#[derive(Default)]
struct ZeroCapacityShard;
impl Shard for ZeroCapacityShard {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "zero_capacity_shard"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn mailbox_capacity() -> usize {
        0
    }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    fn run(&mut self, _: String, _: StateHandle<'_, Self>, _: &mut ShardCtx<'_>) -> Result<(), ShardError> {
        Ok(())
    }
}

struct ZeroCapacityRelay;
impl Relay for ZeroCapacityRelay {
    fn factory_id() -> &'static str {
        "zero_capacity_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("inp").into::<ZeroCapacityShard>();
    }
}

#[test]
fn capacity_zero_is_rejected_at_relay_startup() {
    let err = build_topology_for::<ZeroCapacityRelay>()
        .err()
        .expect("expected mailbox-capacity-zero rejection");
    let msg = format!("{:?}", err.errors);
    assert!(
        msg.contains("MailboxCapacityZero"),
        "expected MailboxCapacityZero error, got {msg}"
    );
    assert!(
        msg.contains("zero_capacity_shard"),
        "expected offending Shard name in error, got {msg}"
    );
}

/// Shard with explicit override of 4096 — the resolved capacity must
/// equal that value when threaded through `build_topology`.
#[derive(Default)]
struct ExplicitCapShard;
impl Shard for ExplicitCapShard {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "explicit_cap_shard"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn mailbox_capacity() -> usize {
        4096
    }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    fn run(&mut self, _: String, _: StateHandle<'_, Self>, _: &mut ShardCtx<'_>) -> Result<(), ShardError> {
        Ok(())
    }
}

struct ExplicitCapRelay;
impl Relay for ExplicitCapRelay {
    fn factory_id() -> &'static str {
        "explicit_cap_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("inp").into::<ExplicitCapShard>();
    }
}

#[test]
fn explicit_capacity_threads_to_topology_descriptor() {
    let topology = build_topology_for::<ExplicitCapRelay>().expect("topology");
    assert_eq!(topology.shards.len(), 1);
    assert_eq!(topology.shards[0].mailbox_capacity, 4096);
    assert!(topology.shards[0].mailbox_capacity_overridden);
}

/// Default Shard inherits the 1024 default.
#[test]
fn default_capacity_is_1024_in_topology() {
    let topology = build_topology_for::<SingleRelay>().expect("topology");
    assert_eq!(topology.shards[0].mailbox_capacity, 1024);
    assert!(!topology.shards[0].mailbox_capacity_overridden);
}

#[test]
fn relay_instance_creates_partitions() {
    use flezha_executor::__dispatch_internals as bi;
    let topology = build_topology_for::<StaticRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(2));
    let instance = flezha_executor::relay::RelayInstance::new(
        topology,
        Arc::from("instance-id"),
        pool,
    );
    assert_eq!(instance.partitions.len(), 1);
    assert_eq!(instance.partitions[0].len(), 3);
    assert_eq!(instance.partition_key_to_idx[0].get("a"), Some(&0));
    assert_eq!(instance.partition_key_to_idx[0].get("b"), Some(&1));
    assert_eq!(instance.partition_key_to_idx[0].get("c"), Some(&2));
}
