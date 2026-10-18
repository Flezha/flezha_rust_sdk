//! The reserved keys this SDK stamps on an output, held to the shared
//! constants fixture.

use flezha_runner::testing::TaskletTestHarness;

mod common;
use common::contract::contract_file;
use common::prelude::*;

#[derive(Default)]
struct KeyedEmit;
#[flezha_runner::tasklet(name = "wire_constants_keyed_emit")]
impl Tasklet for KeyedEmit {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit_keyed(input.clone(), input)?;
        Ok(())
    }
}

#[test]
fn every_stamped_key_is_a_reserved_key_of_the_fixture() {
    let fixture =
        std::fs::read_to_string(contract_file("test/fixtures/wire_constants.txt")).unwrap();
    let reserved: Vec<&str> = fixture
        .lines()
        .filter_map(|line| line.strip_prefix("flz_key|"))
        .filter_map(|row| row.split('|').nth(1))
        .collect();

    let mut harness = TaskletTestHarness::<KeyedEmit>::new().with_keyed_channel();
    harness.apply_inputs(vec!["BTC/1m".to_string()]);
    let metas = harness.emitted_meta();
    let meta = metas[0].expect("a keyed emit records its stamps");
    let stamped: Vec<&String> = meta.keys().filter(|key| key.starts_with("flz.")).collect();

    assert_eq!(
        stamped.len(),
        3,
        "a keyed emit carries its cell, seq and key stamps"
    );
    for key in stamped {
        assert!(
            reserved.contains(&key.as_str()),
            "{key} is stamped but is no flz_key row"
        );
    }
}
