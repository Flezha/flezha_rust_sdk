//! Per-Partition mailboxes backed by a DashMap registry and a Chase-Lev
//! work-stealing pool. The coordinator thread still exists for control-plane
//! traffic (`HandleCommandAction`, `ReturnRelays`, `Reconnect`) but
//! data-plane traffic bypasses it entirely — stream readers push directly
//! into the targeted Relay instance's partition mailboxes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};

use crossbeam::channel::{Receiver, Sender};
use flz_transport::transport::quic::QuicClient;
use flz_transport::transport::FlezhaStream;

use super::work_stealing_pool::WorkStealingPool;
use super::{control_unit, ClientResponseMessage, ExecutorMessage, ExecutorStatus};
use crate::command;
use crate::relay::instance::RelayInstance;
use crate::relay::registry::{get_class as get_relay_class, RelayInstanceRegistry};
use crate::shutdown::ShutdownBridge;

pub struct ExecutorNode {
    executor_id: String,
    _namespace: String,
    relay_registry: Arc<RelayInstanceRegistry>,
    pool: Arc<WorkStealingPool>,
    shared_io_streams: Arc<RwLock<HashMap<u32, FlezhaStream>>>,
    quic_client: Option<Arc<QuicClient>>,
    status: ExecutorStatus,
}

impl ExecutorNode {
    pub fn new(namespace: String) -> ExecutorNode {
        let relay_registry = Arc::new(RelayInstanceRegistry::new());
        let pool = Arc::new(WorkStealingPool::new());
        let shared_io_streams = Arc::new(RwLock::new(HashMap::new()));

        ExecutorNode {
            executor_id: "undefined".to_string(),
            _namespace: namespace,
            relay_registry,
            pool,
            shared_io_streams,
            quic_client: None,
            status: ExecutorStatus::Starting,
        }
    }

    /// Spawn a Relay instance from a registered Relay class. `factory_id`
    /// is looked up in the global class registry; `instance_id` is what
    /// the wire calls `relay_id`. Idempotent on duplicate
    /// `instance_id`: returns the existing instance.
    pub fn start_relay(
        &self,
        factory_id: String,
        instance_id: String,
    ) -> Option<Arc<RelayInstance>> {
        if let Some(existing) = self.relay_registry.get(&instance_id) {
            return Some(existing);
        }
        let topology = get_relay_class(&factory_id)?;
        let instance = RelayInstance::new(
            topology,
            Arc::from(instance_id.as_str()),
            Arc::clone(&self.pool),
        );
        Some(self.relay_registry.register(instance))
    }

    pub fn stop_relay(&self, instance_id: &str) -> bool {
        self.relay_registry.remove(instance_id).is_some()
    }

    pub fn has_relay(&self, instance_id: &str) -> bool {
        self.relay_registry.contains(instance_id)
    }

    pub fn get_relay(&self, instance_id: &str) -> Option<Arc<RelayInstance>> {
        self.relay_registry.get(instance_id)
    }

    pub fn relay_dispatch_handle(&self) -> RelayDispatchHandle {
        RelayDispatchHandle {
            registry: Arc::clone(&self.relay_registry),
        }
    }

    pub fn _get_namespace(&self) -> String {
        self._namespace.clone()
    }

    /// Public-API view: snapshot of live Relay instances. Mirrors the
    /// pre-`relay-mvp-rust` `Flezha::get_relays()` shape so external
    /// inspectors that walk the registry continue to work.
    // reason: returned shape is the public API; splitting it would break external inspectors
    #[allow(clippy::type_complexity)]
    pub fn get_relays(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<Arc<RelayInstance>>>>>> {
        crate::relay::registry::snapshot_as_relay_view(&self.relay_registry)
    }

    pub fn get_io_streams(&self) -> Arc<RwLock<HashMap<u32, FlezhaStream>>> {
        Arc::clone(&self.shared_io_streams)
    }

    pub fn get_executor_id(&self) -> String {
        self.executor_id.clone()
    }

    pub fn assign_executor_id(&mut self, executor_id: String) {
        self.executor_id = executor_id;
    }

    pub fn set_status(&mut self, status: ExecutorStatus) {
        self.status = status;
    }

    pub fn get_quic_client(&self) -> Option<Arc<QuicClient>> {
        self.quic_client.clone()
    }

    pub fn set_quic_client(&mut self, client: Option<Arc<QuicClient>>) {
        self.quic_client = client;
    }
}

pub fn start(
    namespace: String,
    (otx, orx): (Sender<ExecutorMessage>, Receiver<ExecutorMessage>),
    ctx: Sender<ClientResponseMessage>,
    shutdown_bridge: Arc<ShutdownBridge>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut executor = ExecutorNode::new(namespace.clone());

        // Capture a registry snapshot closure now so the shutdown bridge
        // can enumerate live Relay instances without going through the
        // coordinator channel. Survives reconnects since the registry
        // Arc is stable.
        {
            let registry = Arc::clone(&executor.relay_registry);
            shutdown_bridge.set_snapshot_fn(Box::new(move || {
                registry
                    .snapshot()
                    .into_iter()
                    .map(|(k, instance)| {
                        (
                            k,
                            crate::command::partition_state::serialize_relay_state(&instance),
                        )
                    })
                    .collect()
            }));
        }

        let ctx_arc = Arc::new(Mutex::new(ctx));
        let otx_arc = Arc::new(Mutex::new(otx));
        let (_control_thread, mut control_stream, quic_client) =
            control_unit::open(namespace.clone(), ctx_arc.clone(), otx_arc.clone());
        executor.set_quic_client(quic_client);
        shutdown_bridge.set_control_stream(Some(Arc::clone(&control_stream)));

        loop {
            let message = orx.recv().expect("Failed to receive coordinator message");

            match message {
                ExecutorMessage::HandleCommandAction(payload_id, message, _metadata) => {
                    command::handle_command(&mut executor, &control_stream, &payload_id, &message);
                }
                ExecutorMessage::ReturnRelays => {
                    return_relays(&executor, ctx_arc.clone());
                }
                ExecutorMessage::Reconnect => {
                    if matches!(executor.status, ExecutorStatus::Stopped) {
                        continue;
                    }
                    handle_reconnect(&mut executor, &mut control_stream, &namespace, &otx_arc);
                    shutdown_bridge.set_control_stream(Some(Arc::clone(&control_stream)));
                }
            }
        }
    })
}

fn return_relays(executor: &ExecutorNode, ctx_arc: Arc<Mutex<Sender<ClientResponseMessage>>>) {
    let relays = executor.get_relays();
    ctx_arc
        .lock()
        .unwrap()
        .send(ClientResponseMessage::ReturnRelays(relays))
        .expect("Failed to send relays response");
}

fn handle_reconnect(
    executor: &mut ExecutorNode,
    control_stream: &mut Arc<FlezhaStream>,
    namespace: &str,
    otx_arc: &Arc<Mutex<Sender<ExecutorMessage>>>,
) {
    executor.set_status(ExecutorStatus::Stopped);

    executor.shared_io_streams.write().unwrap().clear();
    executor.relay_registry.clear();
    executor.assign_executor_id("undefined".to_string());

    let (_, new_stream, new_quic) =
        control_unit::open_reconnect(namespace.to_string(), otx_arc.clone());

    *control_stream = new_stream;
    executor.set_quic_client(new_quic);
}

/// Stream-reader handle for the Relay model. Cheap to clone; carries a
/// reference to the live Relay-instance registry. Per-Partition drain
/// scheduling uses the pool held by `PartitionSlot`.
#[derive(Clone)]
pub struct RelayDispatchHandle {
    pub registry: Arc<RelayInstanceRegistry>,
}

impl RelayDispatchHandle {
    /// Dispatch a frame from the wire. Resolves the entry channel by
    /// `metadata["channel"]` (or the Relay's first declared input
    /// channel as the documented default), wraps the payload bytes via
    /// the channel's `wrap_input`, and pushes the typed
    /// `Arc<dyn Any + Send + Sync>` onto every consumer Shard's
    /// partition mailbox.
    pub fn dispatch(
        &self,
        instance_id: &str,
        payload: Vec<u8>,
        metadata: &HashMap<String, String>,
        stream: Arc<FlezhaStream>,
    ) {
        let instance = match self.registry.get(instance_id) {
            Some(i) => i,
            None => {
                eprintln!("[flezha] relay instance {} not found", instance_id);
                return;
            }
        };

        let topology = &instance.topology;

        let channel_id = match metadata.get("channel") {
            Some(name) => match topology.lookup_channel_id(name) {
                Some(id) => id,
                None => {
                    eprintln!("[flezha] relay {} has no channel `{}`", instance_id, name);
                    return;
                }
            },
            None => {
                match topology
                    .channels
                    .iter()
                    .position(|c| c.kind == crate::channel::ChannelKind::Entry)
                {
                    Some(idx) => idx as u32,
                    None => {
                        eprintln!("[flezha] relay {} has no entry channels", instance_id);
                        return;
                    }
                }
            }
        };

        let channel = match topology.channel(channel_id) {
            Some(c) => c,
            None => return,
        };

        let typed_payload = match &channel.wrap_input {
            Some(wrap) => match wrap(payload) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!(
                        "[flezha] relay {} channel `{}`: wrap_input failed: {}",
                        instance_id, channel.name, e
                    );
                    return;
                }
            },
            None => {
                eprintln!(
                    "[flezha] relay {} channel `{}` has no entry wrapper",
                    instance_id, channel.name
                );
                return;
            }
        };

        let policy = channel.overflow_policy;

        for &consumer_shard_id in &channel.consumers {
            let consumer = match topology.shard(consumer_shard_id) {
                Some(s) => s,
                None => continue,
            };
            let partitions = match instance.partitions.get(consumer_shard_id as usize) {
                Some(p) => p,
                None => continue,
            };
            if consumer.partition_keys.is_empty() {
                if let Some(slot) = partitions.first() {
                    crate::relay::dispatcher::enqueue(
                        slot,
                        build_payload(&typed_payload, &stream),
                        policy,
                    );
                }
            } else {
                let key_to_idx = instance
                    .partition_key_to_idx
                    .get(consumer_shard_id as usize);
                let keys = (consumer.partition_for_erased)(&*typed_payload);
                for key in keys {
                    if let Some(map) = key_to_idx {
                        if let Some(&idx) = map.get(key) {
                            if let Some(slot) = partitions.get(idx) {
                                crate::relay::dispatcher::enqueue(
                                    slot,
                                    build_payload(&typed_payload, &stream),
                                    policy,
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

fn build_payload(
    value: &Arc<dyn std::any::Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
) -> crate::partition::PartitionPayload {
    crate::partition::PartitionPayload {
        value: Arc::clone(value),
        stream: Arc::clone(stream),
    }
}
