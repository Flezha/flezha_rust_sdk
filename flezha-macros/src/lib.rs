//! Proc-macros realizing the Flezha SDK authoring contract for Rust.
//!
//! - `#[flezha::shard]` — attribute macro on a `run` function; infers the six
//!   `Shard` associated types from the signature and generates the impl, the
//!   PascalCase shard type, and (for the single-shard `input → sink` case) a
//!   `Relay` impl plus inventory auto-registration.
//! - `#[derive(flezha::State)]` — generates a field-granular `Change` enum +
//!   `apply_change` + journaling mutators on the state handle.
//! - `flezha::relay!` — declarative macro lowering a strictly linear pipeline
//!   to a `channels()` body + auto-registration.
//!
//! Everything expands to the existing trait surface; see
//! `docs/sdk_authoring_contract.md`.

use proc_macro::TokenStream;

mod relay;
mod shard;
mod state;
mod util;

/// `#[flezha::shard(...)]` attribute macro. See module docs.
#[proc_macro_attribute]
pub fn shard(attr: TokenStream, item: TokenStream) -> TokenStream {
    shard::expand(attr.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// `#[derive(flezha::State)]` derive macro. See module docs.
#[proc_macro_derive(State, attributes(journal))]
pub fn derive_state(item: TokenStream) -> TokenStream {
    state::expand(item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// `flezha::relay! { id: "...", pipeline: ... }` declarative macro.
#[proc_macro]
pub fn relay(item: TokenStream) -> TokenStream {
    relay::expand(item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
