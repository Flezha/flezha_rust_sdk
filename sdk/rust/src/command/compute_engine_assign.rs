use super::CommandAction;
use crate::{
    compute_engine::{ComputeEngineNode, ComputeEngineStatus},
    pb::AssignComputeEngineId,
};

impl CommandAction for AssignComputeEngineId {
    fn handle(
        &self,
        compute_engine: &mut ComputeEngineNode,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        compute_engine.assign_compute_engine_id(self.compute_engine_id.clone());
        compute_engine.set_status(ComputeEngineStatus::Running);

        ("assign_compute_engine_id".to_string(), Vec::new())
    }
}
