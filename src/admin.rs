//! Administering the namespace this runner's connection proved, from ordinary
//! application code.
//!
//! [`FlezhaRunner::admin`](crate::FlezhaRunner::admin) hands back an [`Admin`]
//! whose methods start, stop and feed operatives, enable and disable plugins,
//! and read what is running. No procedure has to be running and the
//! application holds no credential of its own: the controller judges every
//! call as the identity the connection already established, so an operator
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
use serde_json::{json, Value};

/// The specification a start takes, the same one `flz operative start` takes.
#[derive(Debug, Clone, Default)]
pub struct OperativeSpec {
    id: String,
    factory_id: String,
    kind: Option<String>,
    init_values: Value,
    opts: Value,
}

impl OperativeSpec {
    /// A stream operative `id` served by the factory `factory_id`.
    pub fn new(id: impl Into<String>, factory_id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            factory_id: factory_id.into(),
            kind: None,
            init_values: Value::Null,
            opts: Value::Null,
        }
    }

    /// The operative kind: `"stream"` (the default) or `"workflow"`.
    pub fn kind(mut self, kind: impl Into<String>) -> Self {
        self.kind = Some(kind.into());
        self
    }

    /// The values the factory is seeded with, keyed by procedure.
    pub fn init_values(mut self, values: Value) -> Self {
        self.init_values = values;
        self
    }

    /// The start options, as `flz operative start` takes them.
    pub fn opts(mut self, opts: Value) -> Self {
        self.opts = opts;
        self
    }

    fn to_fields(&self) -> Value {
        let mut fields = json!({ "id": self.id, "factory_id": self.factory_id });
        let object = fields
            .as_object_mut()
            .expect("a JSON object was just built");
        if let Some(kind) = &self.kind {
            object.insert("kind".into(), json!(kind));
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

/// The administration surface for the namespace this runner proved.
#[derive(Debug, Clone, Copy)]
pub struct Admin;

impl Admin {
    /// Start an operative. Answers the controller's own result.
    pub fn start_operative(&self, spec: &OperativeSpec) -> Result<Value, AdminError> {
        self.call("Operative.start", &[spec.to_fields()])
    }

    /// Stop an operative, or the whole partition group it names.
    pub fn stop_operative(&self, operative_id: &str) -> Result<Value, AdminError> {
        self.call("Operative.stop", &[json!(operative_id)])
    }

    /// Inject a message into an operative's input stream and block for the
    /// delivery verdict. `channel` may be empty for none.
    pub fn send(
        &self,
        operative_id: &str,
        message: &str,
        channel: &str,
    ) -> Result<Value, AdminError> {
        self.call(
            "Operative.send",
            &[json!(operative_id), json!(message), json!(channel)],
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

    /// The operative ids running in this namespace.
    pub fn list_operatives(&self) -> Result<Value, AdminError> {
        self.call("Operative.list_operatives", &[])
    }

    /// The plugins enabled on the controller node answering this call.
    pub fn list_plugins(&self) -> Result<Value, AdminError> {
        self.call("PluginManager.list", &[])
    }

    /// One entity's live state, and the change sequence it was read at.
    pub fn entity_state(&self, operative_id: &str, entity_key: &str) -> Result<Value, AdminError> {
        self.call(
            "Operative.entity_state",
            &[json!(operative_id), json!(entity_key)],
        )
    }

    /// Any published function, by name, for a verb this surface has not yet
    /// grown a method for. What succeeds follows from the role the namespace
    /// is bound to, never from a list kept here.
    pub fn call(&self, verb: &str, args: &[Value]) -> Result<Value, AdminError> {
        let args = Value::Array(args.to_vec()).to_string();
        let answer = admin::call(verb, &args)?;
        serde_json::from_str(&answer)
            .map_err(|e| AdminError::Refused(format!("controller answered unreadable JSON: {e}")))
    }
}
