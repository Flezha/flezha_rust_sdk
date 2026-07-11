use super::CommandAction;
use crate::executor::ExecutorNode;
use crate::pb::{AddOperative, AddOperativeResponse};
use prost::Message;

impl CommandAction for AddOperative {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let instance_id = self.operative_id.clone();
        let factory_id = self.factory_id.clone();

        // Empty kind ⇒ `unbounded`, the wire default.
        let kind = if self.kind.is_empty() {
            "unbounded"
        } else {
            self.kind.as_str()
        };

        // Idempotent on reconnect: the controller re-issues AddOperative for
        // every UnboundedOperative instance ProcessHub still has tagged to this
        // executor's profile after a restart. If the instance is already
        // in the local registry, skip without re-registering — keeps
        // the existing per-Segment state intact and prevents
        // duplicate-init crashes.
        if executor.has_operative(&instance_id) {
            return (
                "start_operative".to_string(),
                metadata_response(kind, &factory_id),
            );
        }

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

        (
            "start_operative".to_string(),
            metadata_response(kind, &factory_id),
        )
    }
}

/// Encode the class's SDK-side declarations as `AddOperativeResponse`
/// metadata for the controller's capability resolution. Returns empty
/// bytes when nothing is declared (the default), keeping the response
/// byte-identical to pre-tier builds.
fn metadata_response(kind: &str, factory_id: &str) -> Vec<u8> {
    let effectively_once = match kind {
        "bounded" => crate::bounded::registry::get_class(factory_id)
            .and_then(|spec| spec.topology.as_ref().map(|t| t.effectively_once))
            .unwrap_or(false),
        _ => crate::operative::registry::get_class(factory_id)
            .map(|t| t.effectively_once)
            .unwrap_or(false),
    };
    if !effectively_once {
        return Vec::new();
    }
    let response = AddOperativeResponse {
        metadata: std::collections::HashMap::from([(
            "effectively_once".to_string(),
            "true".to_string(),
        )]),
    };
    response.encode_to_vec()
}
