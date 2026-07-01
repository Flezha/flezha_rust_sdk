use super::CommandAction;
use crate::executor::ExecutorNode;
use crate::pb::BoundedStop;

impl CommandAction for BoundedStop {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        // Cooperative external stop (`flz operative stop`): trip the bounded
        // driver's cancellation token. The driver unwinds in-flight work to the
        // single teardown and emits BoundedCompleted(status="cancelled"). A
        // no-op if the instance is not running (already finished / never
        // started), distinct from the hard `RemOperative` removal.
        if let Some(instance) = executor.get_bounded(&self.operative_id) {
            instance.request_cancel();
        }
        ("bounded_stop".to_string(), Vec::new())
    }
}
