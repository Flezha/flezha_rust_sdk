// Verifies the legacy `RelayHandler` / `register_factory` /
// `FactoryMetadata` API is no longer importable after the
// operative-mvp-rust cutover. Compilation MUST fail at the `use` line.

use flezha_executor::RelayHandler;

fn main() {}
