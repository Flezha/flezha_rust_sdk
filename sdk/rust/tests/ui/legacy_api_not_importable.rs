// Verifies the legacy `ProcessorHandler` / `register_factory` /
// `FactoryMetadata` API is no longer importable after the
// relay-mvp-rust cutover. Compilation MUST fail at the `use` line.

use flezha_compute_engine::ProcessorHandler;

fn main() {}
