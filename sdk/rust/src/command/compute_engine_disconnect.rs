use super::CommandAction;
use crate::{
    compute_engine::{ComputeEngineNode, ComputeEngineStatus},
    pb::ComputeEngineDisconnect,
};

impl CommandAction for ComputeEngineDisconnect {
    fn handle(
        &self,
        compute_engine: &mut ComputeEngineNode,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        compute_engine.set_status(ComputeEngineStatus::Stopped);

        ("compute_engine_disconnect".to_string(), Vec::new())
    }
}
