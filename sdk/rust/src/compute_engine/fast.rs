//! Fast-dispatch path. Per-processor mailboxes backed by a DashMap registry
//! and a Chase-Lev work-stealing pool. Replaces the single-coordinator
//! dispatch loop that gates the legacy path on `ComputeEngineMessage::
//! WorkerCheckOut` / `WorkerCheckIn` / `WorkerInitCheckIn`.
//!
//! The coordinator thread still exists — it's needed for control-plane
//! messages (`HandleCommandAction`, `ReturnProcessors`, `Reconnect`) — but
//! data-plane traffic bypasses it entirely. Stream readers push into the
//! processor's mailbox directly (see `command::streams_connect::listen_incoming_*`).

#![cfg(feature = "fast-dispatch")]

use crossbeam::channel::{Receiver, Sender};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};

use flz_transport::transport::FlezhaStream;
use flz_transport::transport::quic::QuicClient;

use crate::command;
use crate::processor::factory_registry::{
    get_factory, ProcessorBox, ProcessorHandle, ProcessorRegistry, ProcessorSlot,
};
use crate::processor::executor;
use crate::processor::ProcessorRunner;
use crate::processor::ProcessorResult;
use crate::ProcessorInput;

use super::mailbox;
use super::work_stealing_pool::WorkStealingPool;
use super::{ClientResponseMessage, ComputeEngineMessage, ComputeEngineStatus};
use super::control_unit;

pub struct ComputeEngineNode {
    compute_engine_id: String,
    _namespace: String,
    registry: Arc<ProcessorRegistry>,
    pool: Arc<WorkStealingPool>,
    shared_io_streams: Arc<RwLock<HashMap<u32, FlezhaStream>>>,
    /// Control-plane channel kept for parity with the legacy API surface;
    /// data-plane traffic does not go through this channel under fast-dispatch.
    #[allow(dead_code)]
    compute_engine_channel: Arc<Mutex<Sender<ComputeEngineMessage>>>,
    quic_client: Option<Arc<QuicClient>>,
    status: ComputeEngineStatus,
}

impl ComputeEngineNode {
    pub fn new(namespace: String, otx: Sender<ComputeEngineMessage>) -> ComputeEngineNode {
        let registry = Arc::new(ProcessorRegistry::new());
        let pool = Arc::new(WorkStealingPool::new());
        let shared_io_streams = Arc::new(RwLock::new(HashMap::new()));
        let otx_arc = Arc::new(Mutex::new(otx));

        ComputeEngineNode {
            compute_engine_id: "undefined".to_string(),
            _namespace: namespace,
            registry,
            pool,
            shared_io_streams,
            compute_engine_channel: otx_arc,
            quic_client: None,
            status: ComputeEngineStatus::Starting,
        }
    }

    pub fn start_processor(
        &self,
        factory_id: String,
        processor_id: String,
    ) -> Option<ProcessorHandle> {
        if let Some(factory) = get_factory(factory_id) {
            let processor = factory(processor_id.clone());
            let slot = self.registry.register(processor_id, processor);
            Some(ProcessorHandle {
                processor: Arc::clone(&slot.processor),
            })
        } else {
            None
        }
    }

    pub fn stop_processor(&self, id: String) {
        let _ = self.registry.remove(&id);
    }

    pub fn _get_namespace(&self) -> String {
        self._namespace.clone()
    }

    /// Public-API compat: returns an `Arc<RwLock<HashMap<...>>>` view built
    /// from a point-in-time snapshot of the DashMap. Callers use this for
    /// inspection/iteration; mutations go through `start_processor` /
    /// `stop_processor` / `AddProcessor` / `RemProcessor` as before.
    pub fn get_processors(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<ProcessorBox>>>>> {
        Arc::new(RwLock::new(self.registry.snapshot()))
    }

    pub fn get_processor(&self, id: String) -> Option<ProcessorHandle> {
        self.registry
            .get_processor(&id)
            .map(|processor| ProcessorHandle { processor })
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

    pub fn has_processor(&self, id: String) -> bool {
        self.registry.contains(&id)
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

    /// Build a cheap handle for stream readers to push into mailboxes.
    pub fn fast_handle(&self) -> FastDispatchHandle {
        FastDispatchHandle {
            registry: Arc::clone(&self.registry),
            pool: Arc::clone(&self.pool),
        }
    }
}

pub fn start(
    namespace: String,
    (otx, orx): (Sender<ComputeEngineMessage>, Receiver<ComputeEngineMessage>),
    ctx: Sender<ClientResponseMessage>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut compute_engine = ComputeEngineNode::new(namespace.clone(), otx.clone());
        let ctx_arc = Arc::new(Mutex::new(ctx));
        let otx_arc = Arc::new(Mutex::new(otx));
        let (_control_thread, mut control_stream, quic_client) =
            control_unit::open(namespace.clone(), ctx_arc.clone(), otx_arc.clone());
        compute_engine.set_quic_client(quic_client);

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
    compute_engine.registry.clear();
    compute_engine.assign_compute_engine_id("undefined".to_string());

    let (_, new_stream, new_quic) =
        control_unit::open_reconnect(namespace.to_string(), otx_arc.clone());

    *control_stream = new_stream;
    compute_engine.set_quic_client(new_quic);
}

// ---------------------------------------------------------------------------
// Stream-reader hot path.
//
// Called from `command::streams_connect::listen_incoming_uds`/`_quic`. Looks
// up the processor via the DashMap, dispatches to the right runner mode,
// and pushes into the mailbox on ThreadPool / spawns a thread on ThreadSpawn.
// ---------------------------------------------------------------------------

/// Cheap-to-clone handle for stream readers.
#[derive(Clone)]
pub struct FastDispatchHandle {
    pub registry: Arc<ProcessorRegistry>,
    pub pool: Arc<WorkStealingPool>,
}

impl FastDispatchHandle {
    pub fn dispatch(&self, processor_id: String, input: ProcessorInput, stream: Arc<FlezhaStream>) {
        let slot = match self.registry.get(&processor_id) {
            Some(s) => s,
            None => {
                eprintln!("[flezha] Processor {} not found", processor_id);
                return;
            }
        };

        match slot.runner_type.clone() {
            ProcessorRunner::ThreadPool => {
                dispatch_thread_pool(&slot, processor_id, input, stream, self);
            }
            ProcessorRunner::ThreadSpawn => {
                dispatch_thread_spawn(&slot, processor_id, input, stream);
            }
        }
    }
}

fn dispatch_thread_pool(
    slot: &Arc<ProcessorSlot>,
    _processor_id: String,
    input: ProcessorInput,
    stream: Arc<FlezhaStream>,
    handle: &FastDispatchHandle,
) {
    // Hot path: only a lock-free queue push + an atomic CAS. No closure
    // allocation on the common case where someone else is already draining.
    slot.mailbox.push_only(mailbox::ProcessorPayload { input, stream });

    if slot.mailbox.try_mark_inflight() {
        let slot_cloned = Arc::clone(slot);
        let handle_cloned = handle.clone();
        handle
            .pool
            .submit(Box::new(move || drain_slot(&slot_cloned, &handle_cloned)));
    }
}

/// Drain loop for a single processor's mailbox. Invariant: at most one
/// call is active per slot at any moment (enforced by `inflight`).
fn drain_slot(slot: &Arc<ProcessorSlot>, handle: &FastDispatchHandle) {
    loop {
        while let Some(payload) = slot.mailbox.pop() {
            let mailbox::ProcessorPayload { input, stream } = payload;
            let result = {
                let mut p = slot.processor.lock().unwrap();
                p.run(input)
            };
            handle_drain_result(&slot.processor_id, result, &stream, handle);
        }

        slot.mailbox.release_inflight();
        if slot.mailbox.is_empty() {
            return;
        }
        // Missed-wake recovery: someone pushed while we were releasing. If
        // we can re-acquire, resume; otherwise another drain already took over.
        if !slot.mailbox.try_mark_inflight() {
            return;
        }
    }
}

fn handle_drain_result(
    processor_id: &Arc<str>,
    result: Result<ProcessorResult, crate::processor::ProcessorError>,
    stream: &Arc<FlezhaStream>,
    handle: &FastDispatchHandle,
) {
    match result {
        Ok(ProcessorResult::None) | Ok(ProcessorResult::Return(_)) => {}
        Ok(ProcessorResult::PipeToAgent(outputs)) => {
            // `processor_id.as_ref()` → `&str`, no allocation. pipe_to_agent
            // now accepts `&str` + `&metadata`, so the whole write path is
            // alloc-free (frame bytes go into a thread-local scratch buffer).
            for (output, metadata) in outputs {
                executor::pipe_to_agent(stream, processor_id.as_ref(), &output, &metadata);
            }
        }
        Ok(ProcessorResult::PipeToProcessor(outputs)) => {
            for (target_id, output, metadata) in outputs {
                if handle.registry.contains(&target_id) {
                    let input = ProcessorInput::from(output);
                    handle.dispatch(target_id, input, Arc::clone(stream));
                } else {
                    executor::pipe_to_agent(stream, &target_id, &output, &metadata);
                }
            }
        }
        Err(e) => eprintln!("Error executing processor {}: {}", processor_id, e),
    }
}


fn dispatch_thread_spawn(
    slot: &Arc<ProcessorSlot>,
    processor_id: String,
    input: ProcessorInput,
    stream: Arc<FlezhaStream>,
) {
    // ThreadSpawn semantics: one OS thread per invocation, no cross-invocation
    // ordering. Matches legacy-path behavior.
    let processor = Arc::clone(&slot.processor);
    thread::spawn(move || {
        let result = {
            let mut p = processor.lock().unwrap();
            p.run(input)
        };
        // Reentrancy guard: PipeToProcessor outputs from a ThreadSpawn runner
        // have nowhere to chain in this function — historically the legacy
        // path also recursed through `exec_agent`, so we emulate by piping
        // the outputs to agent directly (missing processor => pipe to agent).
        match result {
            Ok(ProcessorResult::None) | Ok(ProcessorResult::Return(_)) => {}
            Ok(ProcessorResult::PipeToAgent(outputs)) => {
                for (output, metadata) in outputs {
                    executor::pipe_to_agent(&stream, &processor_id, &output, &metadata);
                }
            }
            Ok(ProcessorResult::PipeToProcessor(outputs)) => {
                // No FastDispatchHandle reachable here (ThreadSpawn does not
                // get one). Fall back to pipe_to_agent for each output, which
                // matches legacy's `processor not found → pipe_to_agent` fallback.
                for (pid, output, metadata) in outputs {
                    executor::pipe_to_agent(&stream, &pid, &output, &metadata);
                }
            }
            Err(e) => eprintln!("Error executing processor {}: {}", processor_id, e),
        }
    });
}

