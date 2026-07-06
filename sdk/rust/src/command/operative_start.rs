use super::CommandAction;
use crate::executor::ExecutorNode;
use crate::pb::AddOperative;

impl CommandAction for AddOperative {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let instance_id = self.operative_id.clone();
        let factory_id = self.factory_id.clone();

        // Idempotent on reconnect: the controller re-issues AddOperative for
        // every UnboundedOperative instance ProcessHub still has tagged to this
        // executor's profile after a restart. If the instance is already
        // in the local registry, skip without re-registering — keeps
        // the existing per-Segment state intact and prevents
        // duplicate-init crashes.
        if executor.has_operative(&instance_id) {
            return ("start_operative".to_string(), Vec::new());
        }

        // Empty kind ⇒ `unbounded`, the wire default.
        let kind = if self.kind.is_empty() {
            "unbounded"
        } else {
            self.kind.as_str()
        };

        match kind {
            "bounded" => {
                if !executor.start_bounded_resumable(
                    factory_id.clone(),
                    instance_id.clone(),
                    self.init_values.clone(),
                    self.resume_segment_count,
                ) {
                    errors.push(format!(
                        "BoundedOperative class with factory_id {} is not registered",
                        factory_id
                    ));
                }
            }
            "unbounded" => {
                match executor.start_operative(factory_id.clone(), instance_id.clone()) {
                    Some(instance) => {
                        if !self.init_values.is_empty() {
                            if let Err(e) = crate::command::segment_state::apply_init_values(
                                &instance,
                                &self.init_values,
                            ) {
                                errors.push(e);
                            }
                        }
                    }
                    None => {
                        errors.push(format!(
                            "UnboundedOperative class with factory_id {} is not registered",
                            factory_id
                        ));
                    }
                }
            }
            other => {
                errors.push(format!(
                    "unknown compute-unit kind '{}' for unit {}",
                    other, instance_id
                ));
            }
        }

        ("start_operative".to_string(), Vec::new())
    }
}
