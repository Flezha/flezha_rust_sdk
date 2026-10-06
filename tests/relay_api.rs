//! A relay's API as the runner announces it: each entry channel with its
//! format, its description and its schema, and the answer of the output it
//! reaches. A schema that cannot be announced refuses the class.

mod common;
use common::prelude::*;
use flezha_runner::channel::ChannelBuilder;
use flezha_runner::relay::builder::build_topology_for;
use flezha_runner::relay::registry::{advertised_for_factory, register_class};
use serde_json::json;

#[derive(Clone, serde::Deserialize, serde::Serialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
struct PlaceOrder {
    pair: String,
    amount: f64,
}

test_tasklet!(Place, "relay_api_place", PlaceOrder => PlaceOrder, (), |input, ctx| {
    ctx.emit(input)
});
test_tasklet!(Audit, "relay_api_audit", String => String, (), |_input, _ctx| {
    Ok(())
});

struct Orders;
impl Relay for Orders {
    fn factory_id() -> &'static str {
        "relay_api_orders"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<PlaceOrder>("place")
            .describe("Places an order")
            .schema(json!({"type": "object", "properties": {"pair": {"type": "string"}}}))
            .into::<Place>()
            .output_named("placed");
        c.input::<String>("audit").into::<Audit>();
    }
}

#[test]
fn the_announcement_carries_each_entry_with_its_answer() {
    register_class(Orders::factory_id(), build_topology_for::<Orders>().expect("topology"));
    let api = advertised_for_factory(Orders::factory_id()).api;

    let place = api.iter().find(|e| e.channel == "place").expect("place");
    assert_eq!(
        (place.codec.as_str(), place.description.as_str()),
        ("json", "Places an order")
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&place.schema_json).unwrap()["properties"]["pair"],
        json!({"type": "string"})
    );
    assert_eq!(place.answer_codec, "json");

    let audit = api.iter().find(|e| e.channel == "audit").expect("audit");
    assert_eq!((audit.codec.as_str(), audit.answer_codec.as_str()), ("text", ""));
}

struct BrokenSchema;
impl Relay for BrokenSchema {
    fn factory_id() -> &'static str {
        "relay_api_broken_schema"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<PlaceOrder>("place")
            .schema(json!("object"))
            .into::<Place>();
    }
}

#[test]
fn a_schema_that_is_not_an_object_with_a_type_refuses_the_class() {
    let err = build_topology_for::<BrokenSchema>().expect_err("a broken schema is refused");
    let rendered = flz_core::host::render_issues(&err.errors);

    assert_eq!(rendered.code, "topology.schema_invalid");
    assert_eq!(rendered.details["channel"], "place");
}

#[cfg(feature = "schemars")]
mod derived {
    use super::*;

    struct Derived;
    impl Relay for Derived {
        fn factory_id() -> &'static str {
            "relay_api_derived"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<PlaceOrder>("place")
                .derive_schema()
                .into::<Place>()
                .output_named("placed")
                .derive_schema();
        }
    }

    #[test]
    fn a_json_schema_type_derives_the_input_and_the_answer_schema() {
        register_class(Derived::factory_id(), build_topology_for::<Derived>().expect("topology"));
        let api = advertised_for_factory(Derived::factory_id()).api;
        let schema: serde_json::Value = serde_json::from_str(&api[0].schema_json).unwrap();
        let answer: serde_json::Value = serde_json::from_str(&api[0].answer_schema_json).unwrap();

        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"]["amount"]["type"], "number");
        assert_eq!(schema["required"], json!(["pair", "amount"]));
        assert_eq!(answer["properties"]["pair"]["type"], "string");
    }
}
