//! Per-Partition mailboxes backed by a DashMap registry and a Chase-Lev
//! work-stealing pool. The coordinator thread still exists for control-plane
//! traffic (`HandleCommandAction`, `ReturnProcessors`, `Reconnect`) but
//! data-plane traffic bypasses it entirely — stream readers push directly
//! into the targeted Relay instance's partition mailboxes.

use crossbeam::channel::{Receiver, Sender};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};

use flz_transport::transport::FlezhaStream;
use flz_transport::transport::quic::QuicClient;

use crate::command;
use crate::relay::instance::RelayInstance;
use crate::relay::registry::{get_class as get_relay_class, RelayInstanceRegistry};
use crate::shutdown::ShutdownBridge;

use super::work_stealing_pool::WorkStealingPool;
use super::{ClientResponseMessage, ComputeEngineMessage, ComputeEngineStatus};
use super::control_unit;

pub struct ComputeEngineNode {
    compute_engine_id: String,
    _namespace: String,
    relay_registry: Arc<RelayInstanceRegistry>,
    pool: Arc<WorkStealingPool>,
    shared_io_streams: Arc<RwLock<HashMap<u32, FlezhaStream>>>,
    /// Control-plane channel kept for parity with the public API surface;
    /// data-plane traffic does not go through this channel.
    #[allow(dead_code)]
    compute_engine_channel: Arc<Mutex<Sender<ComputeEngineMessage>>>,
    quic_client: Option<Arc<QuicClient>>,
    status: ComputeEngineStatus,
}

impl ComputeEngineNode {
    pub fn new(namespace: String, otx: Sender<ComputeEngineMessage>) -> ComputeEngineNode {
        let relay_registry = Arc::new(RelayInstanceRegistry::new());
        let pool = Arc::new(WorkStealingPool::new());
        let shared_io_streams = Arc::new(RwLock::new(HashMap::new()));
        let otx_arc = Arc::new(Mutex::new(otx));

        ComputeEngineNode {
            compute_engine_id: "undefined".to_string(),
            _namespace: namespace,
            relay_registry,
            pool,
            shared_io_streams,
            compute_engine_channel: otx_arc,
            quic_client: None,
            status: ComputeEngineStatus::Starting,
        }
    }

    /// Spawn a Relay instance from a registered Relay class. `factory_id`
    /// is looked up in the global class registry; `instance_id` is what
    /// the wire calls `processor_id`. Idempotent on duplicate
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

    pub fn get_relay_registry(&self) -> Arc<RelayInstanceRegistry> {
        Arc::clone(&self.relay_registry)
    }

    pub fn relay_dispatch_handle(&self) -> RelayDispatchHandle {
        RelayDispatchHandle {
            registry: Arc::clone(&self.relay_registry),
            pool: Arc::clone(&self.pool),
        }
    }

    pub fn _get_namespace(&self) -> String {
        self._namespace.clone()
    }

    /// Public-API view: snapshot of live Relay instances. Mirrors the
    /// pre-`relay-mvp-rust` `Flezha::get_processors()` shape so external
    /// inspectors that walk the registry continue to work.
    pub fn get_processors(
        &self,
    ) -> Arc<RwLock<HashMap<String, Arc<Mutex<Arc<RelayInstance>>>>>> {
        crate::relay::registry::snapshot_as_processor_view(&self.relay_registry)
    }

    pub fn get_io_streams(&self) -> Arc<RwLock<HashMap<u32, FlezhaStream>>> {
        Arc::clone(&self.shared_io_streams)
    }

    pub fn get_compute_engine_id(&self) -> String {
        self.compute_engine_id.clone()
    }

    pub fn assign_compute_engine_id(&mut self, compute_engine_id: String) {
        self.compute_engine_id = compute_engine_id;
    }

    pub fn set_status(&mut self, status: ComputeEngineStatus) {
        self.status = status;
    }

    #[allow(dead_code)]
    pub fn get_compute_engine_channel(&self) -> Arc<Mutex<Sender<ComputeEngineMessage>>> {
        Arc::clone(&self.compute_engine_channel)
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
    (otx, orx): (Sender<ComputeEngineMessage>, Receiver<ComputeEngineMessage>),
    ctx: Sender<ClientResponseMessage>,
    shutdown_bridge: Arc<ShutdownBridge>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut compute_engine = ComputeEngineNode::new(namespace.clone(), otx.clone());

        // Capture a registry snapshot closure now so the shutdown bridge
        // can enumerate live Relay instances without going through the
        // coordinator channel. Survives reconnects since the registry
        // Arc is stable.
        {
            let registry = Arc::clone(&compute_engine.relay_registry);
            shutdown_bridge.set_snapshot_fn(Box::new(move || {
                registry
                    .snapshot()
                    .into_iter()
                    .map(|(k, instance)| {
                        (k, crate::command::processor_state::serialize_relay_state(&instance))
                    })
                    .collect()
            }));
        }

        let ctx_arc = Arc::new(Mutex::new(ctx));
        let otx_arc = Arc::new(Mutex::new(otx));
        let (_control_thread, mut control_stream, quic_client) =
            control_unit::open(namespace.clone(), ctx_arc.clone(), otx_arc.clone());
        compute_engine.set_quic_client(quic_client);
        shutdown_bridge.set_control_stream(Some(Arc::clone(&control_stream)));

        loop {
            let message = orx.recv().expect("Failed to receive coordinator message");

            match message {
                ComputeEngineMessage::HandleCommandAction(payload_id, message, _metadata) => {
                    command::handle_command(
                        &mut compute_engine,
                        &control_stream,
                        &payload_id,
                        &message,
                    );
                }
                ComputeEngineMessage::ReturnProcessors => {
                    return_processors(&compute_engine, ctx_arc.clone());
                }
                ComputeEngineMessage::Reconnect => {
                    if matches!(compute_engine.status, ComputeEngineStatus::Stopped) {
                        continue;
                    }
                    handle_reconnect(
                        &mut compute_engine,
                        &mut control_stream,
                        &namespace,
                        &otx_arc,
                    );
                    shutdown_bridge.set_control_stream(Some(Arc::clone(&control_stream)));
                }
            }
        }
    })
}

fn return_processors(
    compute_engine: &ComputeEngineNode,
    ctx_arc: Arc<Mutex<Sender<ClientResponseMessage>>>,
) {
    let processors = compute_engine.get_processors();
    ctx_arc
        .lock()
        .unwrap()
        .send(ClientResponseMessage::ReturnProcessors(processors))
        .expect("Failed to send processors response");
}

fn handle_reconnect(
    compute_engine: &mut ComputeEngineNode,
    control_stream: &mut Arc<FlezhaStream>,
    namespace: &str,
    otx_arc: &Arc<Mutex<Sender<ComputeEngineMessage>>>,
) {
    compute_engine.set_status(ComputeEngineStatus::Stopped);

    compute_engine.shared_io_streams.write().unwrap().clear();
    compute_engine.relay_registry.clear();
    compute_engine.assign_compute_engine_id("undefined".to_string());

    let (_, new_stream, new_quic) =
        control_unit::open_reconnect(namespace.to_string(), otx_arc.clone());

    *control_stream = new_stream;
    compute_engine.set_quic_client(new_quic);
}

/// Stream-reader handle for the Relay model. Cheap to clone; carries a
/// reference to the live Relay-instance registry and the work-stealing
/// pool used to schedule per-Partition drain tasks.
#[derive(Clone)]
pub struct RelayDispatchHandle {
    pub registry: Arc<RelayInstanceRegistry>,
    pub pool: Arc<WorkStealingPool>,
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
                    eprintln!(
                        "[flezha] relay {} has no channel `{}`",
                        instance_id, name
                    );
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
                        eprintln!(
                            "[flezha] relay {} has no entry channels",
                            instance_id
                        );
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
                    enqueue(slot, build_payload(&typed_payload, &stream));
                }
            } else {
                let key_to_idx =
                    instance.partition_key_to_idx.get(consumer_shard_id as usize);
                let keys = (consumer.partition_for_erased)(&*typed_payload);
                for key in keys {
                    if let Some(map) = key_to_idx {
                        if let Some(&idx) = map.get(key) {
                            if let Some(slot) = partitions.get(idx) {
                                enqueue(slot, build_payload(&typed_payload, &stream));
                            }
                        }
                    }
                }
            }
        }
    }
}

fn enqueue(
    slot: &Arc<crate::partition::PartitionSlot>,
    payload: crate::partition::PartitionPayload,
) {
    slot.mailbox.push_only(payload);
    if slot.mailbox.try_mark_inflight() {
        let cloned = Arc::clone(slot);
        slot.pool.submit(Box::new(move || {
            crate::relay::dispatcher::drain_partition(&cloned)
        }));
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
