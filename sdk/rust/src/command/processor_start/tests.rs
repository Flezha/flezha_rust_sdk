use super::*;

use crate::compute_engine::ComputeEngineNode;
use crate::pb::AddProcessor;
use crate::processor::factory_registry::register_processor_factory;
use crate::processor::{
    Processor, ProcessorError, ProcessorInput, ProcessorResult, ProcessorRunner,
};
use crossbeam::channel::unbounded;

struct StubProcessor {
    state: u32,
}

impl Processor for StubProcessor {
    fn run(&mut self, _input: ProcessorInput) -> Result<ProcessorResult, ProcessorError> {
        Ok(ProcessorResult::None)
    }

    fn json_serialize_state(&self) -> Result<Vec<u8>, String> {
        Ok(Vec::new())
    }

    fn json_deserialize_state(&mut self, _data: &[u8]) -> Result<(), String> {
        Ok(())
    }

    fn get_factory_id(&self) -> String {
        "idempotent_test_factory".to_string()
    }

    fn get_state(&mut self) -> &mut dyn std::any::Any {
        &mut self.state
    }

    fn bin_serialize_state(&self) -> Box<dyn erased_serde::Serialize + '_> {
        Box::new(&self.state)
    }

    fn post_alter(&mut self) {}

    fn get_runner_type(&self) -> ProcessorRunner {
        ProcessorRunner::ThreadSpawn
    }
}

fn ensure_factory_registered() {
    register_processor_factory(
        "idempotent_test_factory".to_string(),
        Box::new(|_id| Box::new(StubProcessor { state: 0 })),
    );
}

#[test]
fn add_processor_skips_when_already_registered() {
    ensure_factory_registered();

    let (otx, _orx) = unbounded();
    let mut compute_engine = ComputeEngineNode::new("test_ns".to_string(), otx);

    let cmd = AddProcessor {
        processor_id: "idempotent_p1".to_string(),
        factory_id: "idempotent_test_factory".to_string(),
        init_values: String::new(),
    };

    let mut errors = Vec::new();
    let _ = cmd.handle(&mut compute_engine, &String::new(), &mut errors);
    assert!(errors.is_empty(), "first call should succeed: {:?}", errors);
    assert!(compute_engine.has_processor("idempotent_p1".to_string()));

    let processors_before = compute_engine.get_processors();
    let len_before = processors_before.read().unwrap().len();

    let _ = cmd.handle(&mut compute_engine, &String::new(), &mut errors);
    assert!(
        errors.is_empty(),
        "duplicate call should not push errors: {:?}",
        errors
    );

    let processors_after = compute_engine.get_processors();
    let len_after = processors_after.read().unwrap().len();
    assert_eq!(
        len_before, len_after,
        "duplicate AddProcessor must not grow the registry"
    );
    assert_eq!(
        len_after, 1,
        "registry should hold exactly one entry for the duplicated processor_id"
    );
}
