//! Proc-macros realizing the Flezha SDK authoring contract for Rust.
//!
//! - `#[flezha::procedure]` — attribute macro on a `run` function; infers the six
//!   `Procedure` associated types from the signature and generates the impl, the
//!   PascalCase procedure type, and (for the single-procedure `input → sink` case) a
//!   `StreamOperative` impl plus inventory auto-registration.
//! - `#[derive(flezha::State)]` — generates a field-granular `Change` enum +
//!   `apply_change` + journaling mutators on the state handle.
//! - `flezha::operative!` — declarative macro lowering a strictly linear pipeline
//!   to a `channels()` body + auto-registration.
//!
//! Everything expands to the existing trait surface; see
//! `docs/sdk/authoring_contract.md`.

use proc_macro::TokenStream;

mod operative;
mod procedure;
mod state;
mod util;

/// `#[flezha::procedure(...)]` attribute macro. See module docs.
#[proc_macro_attribute]
pub fn procedure(attr: TokenStream, item: TokenStream) -> TokenStream {
    procedure::expand(attr.into(), item.into())
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

/// `flezha::operative! { id: "...", pipeline: ... }` declarative macro.
#[proc_macro]
pub fn operative(item: TokenStream) -> TokenStream {
    operative::expand(item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
