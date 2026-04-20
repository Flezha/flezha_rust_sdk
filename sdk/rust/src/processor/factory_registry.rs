use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::processor::{Processor, ProcessorError, ProcessorInput, ProcessorResult};

// Type erased processor for registration. All processors implement Processor.
pub type ProcessorBox = Box<dyn Processor + Send + Sync>;

// Factory function type that can create new processor instances
pub type ProcessorFactory = Box<dyn Fn(String) -> ProcessorBox + Send + Sync>;

// Use Arc<Mutex<ProcessorBox>> for each processor
lazy_static! {
    static ref FACTORY_REGISTRY: RwLock<HashMap<String, Arc<ProcessorFactory>>> =
        RwLock::new(HashMap::new());
    static ref DESCRIPTION_REGISTRY: RwLock<HashMap<String, Option<String>>> =
        RwLock::new(HashMap::new());
}

/// Register a processor factory with a unique factory ID
pub fn register_processor_factory(factory_id: String, factory: ProcessorFactory) {
    FACTORY_REGISTRY
        .write()
        .unwrap()
        .insert(factory_id, Arc::new(factory));
}

pub fn get_factory(factory_id: String) -> Option<Arc<ProcessorFactory>> {
    FACTORY_REGISTRY.read().unwrap().get(&factory_id).cloned()
}

pub fn get_factory_ids() -> Vec<String> {
    FACTORY_REGISTRY.read().unwrap().keys().cloned().collect()
}

pub fn register_factory_description(factory_id: String, description: Option<String>) {
    DESCRIPTION_REGISTRY
        .write()
        .unwrap()
        .insert(factory_id, description);
}

pub fn get_factories() -> HashMap<String, Option<String>> {
    DESCRIPTION_REGISTRY.read().unwrap().clone()
}

/// A wrapper for a processor that handles locking
pub struct ProcessorHandle {
    pub processor: Arc<Mutex<ProcessorBox>>,
}

impl ProcessorHandle {
    /// Get mutable access to the processor
    pub fn processor_mut(&self) -> std::sync::MutexGuard<'_, ProcessorBox> {
        self.processor.lock().unwrap()
    }

    /// Execute the processor with the given input
    pub fn run(&self, input: ProcessorInput) -> Result<ProcessorResult, ProcessorError> {
        self.processor_mut().run(input)
    }
}

// ---------------------------------------------------------------------------
// Fast-dispatch registry: DashMap-backed concurrent processor registry.
//
// Swaps the outer `RwLock<HashMap>` for a sharded concurrent map so many
// stream-reader threads can look up processors without blocking each other.
// The inner `Arc<Mutex<ProcessorBox>>` stays — the `Processor` trait uses
// `&mut self` and the public API keeps `Arc<Mutex<ProcessorBox>>` visible.
//
// Each entry also carries a per-processor mailbox (see `compute_engine::mailbox`)
// so the push path is: `registry.get(id).unwrap().mailbox.push_and_maybe_submit(...)`.
// ---------------------------------------------------------------------------

#[cfg(feature = "fast-dispatch")]
pub struct ProcessorSlot {
    pub processor: Arc<Mutex<ProcessorBox>>,
    pub mailbox: crate::compute_engine::mailbox::ProcessorMailbox,
    /// Runner type cached at registration time so the dispatch hot path
    /// doesn't need to acquire the processor lock just to read it.
    pub runner_type: crate::processor::ProcessorRunner,
    /// Cached processor id string. Lets the drain closure reference the
    /// id by shared pointer without cloning per message.
    pub processor_id: Arc<str>,
}

#[cfg(feature = "fast-dispatch")]
pub struct ProcessorRegistry {
    inner: dashmap::DashMap<String, Arc<ProcessorSlot>>,
}

#[cfg(feature = "fast-dispatch")]
impl ProcessorRegistry {
    pub fn new() -> Self {
        Self {
            inner: dashmap::DashMap::new(),
        }
    }

    pub fn register(&self, id: String, processor: ProcessorBox) -> Arc<ProcessorSlot> {
        let runner_type = processor.get_runner_type();
        let processor_id: Arc<str> = Arc::from(id.as_str());
        let slot = Arc::new(ProcessorSlot {
            processor: Arc::new(Mutex::new(processor)),
            mailbox: crate::compute_engine::mailbox::new_processor_mailbox(),
            runner_type,
            processor_id,
        });
        self.inner.insert(id, Arc::clone(&slot));
        slot
    }

    pub fn get(&self, id: &str) -> Option<Arc<ProcessorSlot>> {
        self.inner.get(id).map(|r| Arc::clone(r.value()))
    }

    pub fn get_processor(&self, id: &str) -> Option<Arc<Mutex<ProcessorBox>>> {
        self.inner.get(id).map(|r| Arc::clone(&r.value().processor))
    }

    pub fn remove(&self, id: &str) -> Option<Arc<ProcessorSlot>> {
        self.inner.remove(id).map(|(_, slot)| slot)
    }

    pub fn contains(&self, id: &str) -> bool {
        self.inner.contains_key(id)
    }

    /// Point-in-time copy of the registry into a plain HashMap. Used by
    /// `Flezha::get_processors()` (which returns an `Arc<RwLock<HashMap>>`
    /// for public-API compatibility) and for `ClientResponseMessage::ReturnProcessors`.
    pub fn snapshot(&self) -> HashMap<String, Arc<Mutex<ProcessorBox>>> {
        self.inner
            .iter()
            .map(|r| (r.key().clone(), Arc::clone(&r.value().processor)))
            .collect()
    }

    pub fn clear(&self) {
        self.inner.clear();
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

#[cfg(feature = "fast-dispatch")]
impl Default for ProcessorRegistry {
    fn default() -> Self {
        Self::new()
    }
}
