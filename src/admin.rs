//! Administering the namespace this runner's connection proved, from ordinary
//! application code.
//!
//! [`FlezhaRunner::admin`](crate::FlezhaRunner::admin) hands back an [`Admin`]
//! whose methods start, stop and feed operators, enable and disable plugins,
//! and read what is running. No tasklet has to be running and the
//! application holds no credential of its own: the controller judges every
//! call as the identity the connection already established, so a user
//! grants the authority once per namespace and revokes it the same way:
//!
//! ```sh
//! flz role bind runner@team-orders app team-orders
//! ```
//!
//! Every method is one line over [`flz_core::admin::call`], because the verb
//! is data on the wire. Adding one costs a method here and nothing native.

use flz_core::admin;
pub use flz_core::admin::AdminError;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};

/// The specification a start takes, the same one `flz operator start` takes.
#[derive(Debug, Clone, Default)]
pub struct OperatorSpec {
    factory_id: String,
    id: Option<String>,
    init_values: Value,
    opts: Value,
}

impl OperatorSpec {
    /// An operator of the factory `factory_id`, of the kind its runner
    /// announced, under an id the controller generates unless [`Self::id`]
    /// names one.
    pub fn new(factory_id: impl Into<String>) -> Self {
        Self {
            factory_id: factory_id.into(),
            id: None,
            init_values: Value::Null,
            opts: Value::Null,
        }
    }

    /// The operator's own id. Without one, every start runs a new operator
    /// under a generated id, so a start retried after a lost answer starts a
    /// second one; with one, a second start leaves the running operator
    /// untouched.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// The values the factory is seeded with, keyed by tasklet.
    pub fn init_values(mut self, values: Value) -> Self {
        self.init_values = values;
        self
    }

    /// The start options, as `flz operator start` takes them.
    pub fn opts(mut self, opts: Value) -> Self {
        self.opts = opts;
        self
    }

    fn to_fields(&self) -> Value {
        let mut fields = json!({ "factory_id": self.factory_id });
        let object = fields
            .as_object_mut()
            .expect("a JSON object was just built");
        if let Some(id) = &self.id {
            object.insert("id".into(), json!(id));
        }
        if !self.init_values.is_null() {
            object.insert("init_values".into(), self.init_values.clone());
        }
        if !self.opts.is_null() {
            object.insert("opts".into(), self.opts.clone());
        }
        fields
    }
}

/// What a start answered: the id the operator runs under, the generated one
/// when the start named none, and what the start did.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct StartResult {
    pub id: String,
    pub outcome: StartOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartOutcome {
    Started,
    /// The named operator was already running and was left untouched.
    AlreadyRunning,
}

/// The administration surface for the namespace this runner proved.
#[derive(Debug, Clone, Copy)]
pub struct Admin;

impl Admin {
    /// Start an operator.
    pub fn start_operator(&self, spec: &OperatorSpec) -> Result<StartResult, AdminError> {
        self.answer("Operator.start", &[spec.to_fields()])
    }

    /// Stop an operator, or the whole partition group it names.
    pub fn stop_operator(&self, operator_id: &str) -> Result<Value, AdminError> {
        self.call("Operator.stop", &[json!(operator_id)])
    }

    /// Inject a message into an operator's input stream and block for the
    /// delivery verdict. `channel` may be empty for none.
    pub fn send(
        &self,
        operator_id: &str,
        message: &str,
        channel: &str,
    ) -> Result<Value, AdminError> {
        self.call(
            "Operator.send",
            &[json!(operator_id), json!(message), json!(channel)],
        )
    }

    /// Enable a plugin, built from the fields `flz plugin enable` takes.
    pub fn enable_plugin(&self, plugin_id: &str, fields: Value) -> Result<Value, AdminError> {
        self.call("PluginManager.enable", &[json!(plugin_id), fields])
    }

    /// Disable a plugin, or every partitioned instance of it.
    pub fn disable_plugin(&self, plugin_id: &str) -> Result<Value, AdminError> {
        self.call("PluginManager.disable", &[json!(plugin_id)])
    }

    /// The operator ids running in this namespace.
    pub fn list_operators(&self) -> Result<Value, AdminError> {
        self.call("Operator.list_operators", &[])
    }

    /// The plugins enabled on the controller node answering this call.
    pub fn list_plugins(&self) -> Result<Value, AdminError> {
        self.call("PluginManager.list", &[])
    }

    /// One cell's live state, and the change sequence it was read at.
    pub fn cell_state(&self, operator_id: &str, cell_key: &str) -> Result<Value, AdminError> {
        self.call(
            "Operator.cell_state",
            &[json!(operator_id), json!(cell_key)],
        )
    }

    /// Any published function, by name, for a verb this surface has not yet
    /// grown a method for. What succeeds follows from the role the namespace
    /// is bound to, never from a list kept here.
    pub fn call(&self, verb: &str, args: &[Value]) -> Result<Value, AdminError> {
        self.answer(verb, args)
    }

    fn answer<T: DeserializeOwned>(&self, verb: &str, args: &[Value]) -> Result<T, AdminError> {
        let args = Value::Array(args.to_vec()).to_string();
        read(verb, &admin::call(verb, &args)?)
    }
}

/// The controller's answer, read as the shape the call promises.
fn read<T: DeserializeOwned>(verb: &str, answer: &str) -> Result<T, AdminError> {
    serde_json::from_str(answer).map_err(|e| {
        AdminError::Refused(flz_core::FlezhaError::with(
            "admin.unreadable_answer",
            [("verb", verb.into()), ("reason", e.to_string().into())],
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_start_names_its_id_only_when_given_one() {
        assert_eq!(
            OperatorSpec::new("echo").to_fields(),
            json!({ "factory_id": "echo" }),
            "no id is sent, so the controller generates one"
        );
        assert_eq!(
            OperatorSpec::new("echo").id("my_echo").to_fields(),
            json!({ "factory_id": "echo", "id": "my_echo" })
        );
    }

    #[test]
    fn a_start_answer_reads_as_its_id_and_outcome() {
        assert_eq!(
            read::<StartResult>(
                "Operator.start",
                r#"{"id":"echo-k7m2qzxw4pd3fn2a","outcome":"started"}"#
            ),
            Ok(StartResult {
                id: "echo-k7m2qzxw4pd3fn2a".to_string(),
                outcome: StartOutcome::Started,
            })
        );
        assert_eq!(
            read::<StartResult>(
                "Operator.start",
                r#"{"id":"my_echo","outcome":"already_running"}"#
            ),
            Ok(StartResult {
                id: "my_echo".to_string(),
                outcome: StartOutcome::AlreadyRunning,
            })
        );
    }

    #[test]
    fn a_start_answer_of_another_shape_is_refused_naming_what_is_wrong() {
        let refused = read::<StartResult>(
            "Operator.start",
            r#"{"id":"my_echo","outcome":"duplicate"}"#,
        );
        assert!(
            matches!(&refused, Err(AdminError::Refused(e))
                if e.code == "admin.unreadable_answer"
                    && e.details["reason"].as_str().is_some_and(|r| r.contains("duplicate"))),
            "{refused:?}"
        );
    }
}
