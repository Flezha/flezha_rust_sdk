//! Administering the namespace this runner's connection proved, from ordinary
//! application code.
//!
//! [`FlezhaRunner::control`](crate::FlezhaRunner::control) hands back a
//! [`Control`] whose methods start and stop relays, enable, bind, unbind
//! and disable plugins, and read what is running. No tasklet has to be running and the
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
pub use flz_core::admin::AdminError as ControlError;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};

/// The specification a start takes, the same one `flz relay start` takes.
#[derive(Debug, Clone, Default)]
pub struct RelaySpec {
    factory_id: String,
    id: Option<String>,
    init_values: Value,
    opts: Value,
}

impl RelaySpec {
    /// A relay of the factory `factory_id`, of the kind its runner
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

    /// The relay's own id. Without one, every start runs a new relay
    /// under a generated id, so a start retried after a lost answer starts a
    /// second one; with one, a second start leaves the running relay
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

    /// The start options, as `flz relay start` takes them.
    pub fn opts(mut self, opts: Value) -> Self {
        self.opts = opts;
        self
    }

    fn to_fields(&self) -> Value {
        given(
            json!({ "factory_id": self.factory_id }),
            [
                ("id", json!(self.id)),
                ("init_values", self.init_values.clone()),
                ("opts", self.opts.clone()),
            ],
        )
    }
}

/// What a plugin enable takes beside the plugin's name, the same as
/// `flz plugin enable` takes.
#[derive(Debug, Clone, Default)]
pub struct PluginSpec {
    provider: String,
    opts: Value,
    single: bool,
    relay_id: Option<String>,
    hooks: Value,
}

impl PluginSpec {
    /// A plugin of `provider`, for example `relay_websocket`, on no options.
    pub fn new(provider: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            ..Self::default()
        }
    }

    /// The provider's options.
    pub fn opts(mut self, opts: Value) -> Self {
        self.opts = opts;
        self
    }

    /// Run one copy in the whole cluster.
    pub fn single(mut self) -> Self {
        self.single = true;
        self
    }

    /// Bind `relay_id` in the same call, with `hooks`: names or
    /// `[name, opts]` pairs.
    pub fn bind(mut self, relay_id: impl Into<String>, hooks: Value) -> Self {
        self.relay_id = Some(relay_id.into());
        self.hooks = hooks;
        self
    }

    fn to_fields(&self) -> Value {
        let opts = if self.opts.is_null() {
            json!({})
        } else {
            self.opts.clone()
        };
        given(
            json!({ "provider": self.provider, "opts": opts }),
            [
                (
                    "single",
                    if self.single {
                        json!(true)
                    } else {
                        Value::Null
                    },
                ),
                ("relay_id", json!(self.relay_id)),
                ("hooks", self.hooks.clone()),
            ],
        )
    }
}

/// `fields` with every one of `optional` that was given, a null being none.
fn given<const N: usize>(mut fields: Value, optional: [(&str, Value); N]) -> Value {
    let object = fields
        .as_object_mut()
        .expect("the fields are a JSON object");
    for (key, value) in optional {
        if !value.is_null() {
            object.insert(key.into(), value);
        }
    }
    fields
}

/// What a start answered: the id the relay runs under, the generated one
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
    /// The named relay was already running and was left untouched.
    AlreadyRunning,
}

/// The administration surface for the namespace this runner proved.
#[derive(Debug, Clone, Copy)]
pub struct Control;

impl Control {
    /// Start a relay.
    pub fn start_relay(&self, spec: &RelaySpec) -> Result<StartResult, ControlError> {
        self.answer("Relay.start", &[spec.to_fields()])
    }

    /// Stop a relay, or the whole partition group it names.
    pub fn stop_relay(&self, relay_id: &str) -> Result<Value, ControlError> {
        self.call("Relay.stop", &[json!(relay_id)])
    }

    /// Enable the plugin `name` in this namespace, as `spec` describes it.
    pub fn enable_plugin(&self, name: &str, spec: &PluginSpec) -> Result<Value, ControlError> {
        self.call("PluginManager.enable", &[json!(name), spec.to_fields()])
    }

    /// Bind the relay `relay_id` to the enabled plugin `name`. `hooks`
    /// may be null and `channel` empty for none.
    pub fn bind_plugin(
        &self,
        name: &str,
        relay_id: &str,
        hooks: Value,
        channel: &str,
    ) -> Result<Value, ControlError> {
        self.call(
            "PluginManager.bind",
            &[json!(name), bind_fields(relay_id, hooks, channel)],
        )
    }

    /// Unbind the relay `relay_id` from the plugin `name`, which stays
    /// enabled.
    pub fn unbind_plugin(&self, name: &str, relay_id: &str) -> Result<Value, ControlError> {
        self.call(
            "PluginManager.unbind",
            &[json!(name), json!({ "relay_id": relay_id })],
        )
    }

    /// Disable the plugin `name`, stopping it wherever it runs.
    pub fn disable_plugin(&self, name: &str) -> Result<Value, ControlError> {
        self.call("PluginManager.disable", &[json!(name), json!({})])
    }

    /// The relays in this namespace across the cluster: `rows`, one row each
    /// (its id, health, runner and node, and a partition group's width and
    /// members), and `unreachable`, the controllers that did not answer.
    pub fn list_relays(&self) -> Result<Value, ControlError> {
        self.call("Relay.list", &[])
    }

    /// The plugins enabled in this namespace, one row each: name, provider,
    /// single, bound relays and where it runs.
    pub fn list_plugins(&self) -> Result<Value, ControlError> {
        self.call("PluginManager.enabled", &[])
    }

    /// A relay's live state, and the change sequence it was read at. `cell`
    /// is `<tasklet>[/<cell_key>]`: a tasklet's Cells, or one of them; `None`
    /// reads every Cell of the relay.
    pub fn cell_state(&self, relay_id: &str, cell: Option<&str>) -> Result<Value, ControlError> {
        self.call("Relay.cell_state", &[json!(relay_id), json!(cell)])
    }

    /// Any published function, by name, for a verb this surface has not yet
    /// grown a method for. What succeeds follows from the role the namespace
    /// is bound to, never from a list kept here.
    pub fn call(&self, verb: &str, args: &[Value]) -> Result<Value, ControlError> {
        self.answer(verb, args)
    }

    fn answer<T: DeserializeOwned>(&self, verb: &str, args: &[Value]) -> Result<T, ControlError> {
        let args = Value::Array(args.to_vec()).to_string();
        read(verb, &admin::call(verb, &args)?)
    }
}

fn bind_fields(relay_id: &str, hooks: Value, channel: &str) -> Value {
    let channel = if channel.is_empty() {
        Value::Null
    } else {
        json!(channel)
    };
    given(
        json!({ "relay_id": relay_id }),
        [("hooks", hooks), ("channel", channel)],
    )
}

/// The controller's answer, read as the shape the call promises.
pub(crate) fn read<T: DeserializeOwned>(verb: &str, answer: &str) -> Result<T, ControlError> {
    serde_json::from_str(answer).map_err(|e| {
        ControlError::Refused(flz_core::FlezhaError::with(
            "control.unreadable_answer",
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
            RelaySpec::new("echo").to_fields(),
            json!({ "factory_id": "echo" }),
            "no id is sent, so the controller generates one"
        );
        assert_eq!(
            RelaySpec::new("echo").id("my_echo").to_fields(),
            json!({ "factory_id": "echo", "id": "my_echo" })
        );
    }

    #[test]
    fn a_plugin_is_enabled_by_name_and_bound_by_relay() {
        assert_eq!(
            PluginSpec::new("relay_file_output")
                .opts(json!({ "path": "out.txt" }))
                .to_fields(),
            json!({ "provider": "relay_file_output", "opts": { "path": "out.txt" } })
        );
        assert_eq!(
            PluginSpec::new("relay_websocket")
                .single()
                .bind("my_echo", json!([["on_output", {}]]))
                .to_fields(),
            json!({
                "provider": "relay_websocket",
                "opts": {},
                "single": true,
                "relay_id": "my_echo",
                "hooks": [["on_output", {}]],
            })
        );
        assert_eq!(
            bind_fields("my_echo", Value::Null, ""),
            json!({ "relay_id": "my_echo" })
        );
        assert_eq!(
            bind_fields("my_echo", json!(["on_output"]), "in"),
            json!({ "relay_id": "my_echo", "hooks": ["on_output"], "channel": "in" })
        );
    }

    #[test]
    fn a_start_answer_reads_as_its_id_and_outcome() {
        assert_eq!(
            read::<StartResult>(
                "Relay.start",
                r#"{"id":"echo-k7m2qzxw4pd3fn2a","outcome":"started"}"#
            ),
            Ok(StartResult {
                id: "echo-k7m2qzxw4pd3fn2a".to_string(),
                outcome: StartOutcome::Started,
            })
        );
        assert_eq!(
            read::<StartResult>(
                "Relay.start",
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
        let refused =
            read::<StartResult>("Relay.start", r#"{"id":"my_echo","outcome":"duplicate"}"#);
        assert!(
            matches!(&refused, Err(ControlError::Refused(e))
                if e.code == "control.unreadable_answer"
                    && e.details["reason"].as_str().is_some_and(|r| r.contains("duplicate"))),
            "{refused:?}"
        );
    }
}
