//! Every public concept spelled as `test/fixtures/sdk_public_names.txt` spells it.
//!
//! A Rust cell is an item path as `public-api.txt` prints it with the generic
//! parameters left out (`flezha_runner::tasklet::ctx::Ctx::replay`), or an
//! attribute key written `#[relay(factory_id)]` / `#[tasklet(cell_keys)]`,
//! which is read from the accepted-key lists the attribute's own compile-fail
//! fixture pins. A cell starting with `!` names something that must not be
//! public. `public-api.txt` is the crate's surface as `cargo public-api`
//! records it, so this holds the fixture and that record equal.

#[path = "common/contract.rs"]
mod contract;

use std::collections::BTreeSet;
use std::path::Path;

const COLUMN: usize = 7; // concept|python|ruby|php|node|elixir|java|rust

fn cells() -> Vec<(String, String)> {
    let fixture = std::fs::read_to_string(contract::contract_file(
        "test/fixtures/sdk_public_names.txt",
    ))
    .expect("the public names fixture");
    fixture
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
        .flat_map(|line| {
            let row: Vec<&str> = line.split('|').collect();
            row[COLUMN]
                .split_whitespace()
                .filter(|path| *path != "-")
                .map(|path| (row[0].to_string(), path.to_string()))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The item path a `public-api.txt` line names, generic parameters left out:
/// `a::B<'x, T>::c(&self) -> D` reads as `a::B::c`.
fn item_path(text: &str) -> String {
    let mut depth = 0usize;
    let mut path = String::new();
    for c in text.chars() {
        match c {
            '<' => depth += 1,
            '>' if depth > 0 => depth -= 1,
            _ if depth > 0 => {}
            c if c.is_alphanumeric() || c == '_' || c == ':' => path.push(c),
            _ => break,
        }
    }
    path
}

/// Every item path `public-api.txt` names.
fn public_items() -> BTreeSet<String> {
    let api = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("public-api.txt"))
        .expect("public-api.txt");
    api.lines()
        .filter(|line| line.starts_with("pub "))
        .filter_map(|line| {
            line.find("flezha_runner")
                .map(|start| item_path(&line[start..]))
        })
        .collect()
}

/// The keys one attribute accepts, as its compile-fail fixture prints them.
fn attribute_keys(attribute: &str) -> BTreeSet<String> {
    let stderr = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/ui/tasklet_relay_keys.stderr"),
    )
    .expect("the attribute keys fixture");
    let marker = format!("#[flezha::{attribute}]: unknown attribute `kind`; accepted: ");
    stderr
        .lines()
        .find_map(|line| line.strip_prefix("error: ")?.strip_prefix(marker.as_str()))
        .unwrap_or_else(|| panic!("no accepted-key list for #[flezha::{attribute}]"))
        .split(", ")
        .map(str::to_string)
        .collect()
}

fn resolves(path: &str, items: &BTreeSet<String>) -> bool {
    match path
        .strip_prefix("#[")
        .and_then(|rest| rest.strip_suffix(")]"))
    {
        Some(attribute) => {
            let (name, key) = attribute
                .split_once('(')
                .expect("an attribute cell names its key");
            attribute_keys(name).contains(key)
        }
        None => items.contains(path),
    }
}

#[test]
fn every_concept_has_its_one_public_spelling() {
    let items = public_items();
    let wrong: Vec<String> = cells()
        .into_iter()
        .filter_map(|(concept, path)| {
            let name = path.trim_start_matches('!');
            let hidden = name != path;
            (resolves(name, &items) == hidden).then(|| {
                format!(
                    "{concept}: `{name}` is {}",
                    if hidden { "public" } else { "missing" }
                )
            })
        })
        .collect();
    assert!(wrong.is_empty(), "{wrong:#?}");
}
