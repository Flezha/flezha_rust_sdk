//! Proc-macros realizing the Flezha SDK authoring contract for Rust.
//!
//! - `#[flezha::procedure]` — attribute macro on a `run` function; infers the six
//!   `Procedure` associated types from the signature and generates the impl and
//!   the PascalCase procedure type.
//! - `#[flezha::stream_operative]` / `#[flezha::workflow_operative]`: the §5
//!   short form on a `run` function, emitting the operative type under the
//!   author's PascalCase name, which the author registers with
//!   `runner.register_factory(Name)`, and the procedure type beside it, suffixed
//!   `Procedure`.
//! - `#[derive(flezha::State)]` — generates a field-granular `Change` enum +
//!   `apply_change` + journaling mutators on the state handle.
//! - `flezha::operative!` — declarative macro lowering a strictly linear pipeline
//!   to a `channels()` body on the type it declares, which the author
//!   registers with `runner.register_factory(Name)`.
//!
//! Everything expands to the existing trait surface; see
//! `docs/sdk/authoring_contract.md`.

use proc_macro::TokenStream;

mod operative;
mod procedure;
mod state;
mod util;

use procedure::Declares;

/// `#[flezha::procedure(...)]` attribute macro. See module docs.
#[proc_macro_attribute]
pub fn procedure(attr: TokenStream, item: TokenStream) -> TokenStream {
    on_run_fn(Declares::Procedure, attr, item)
}

/// `#[flezha::stream_operative(...)]` short-form attribute. See module docs.
#[proc_macro_attribute]
pub fn stream_operative(attr: TokenStream, item: TokenStream) -> TokenStream {
    on_run_fn(Declares::Operative(operative::Kind::Stream), attr, item)
}

/// `#[flezha::workflow_operative(...)]` short-form attribute. See module docs.
#[proc_macro_attribute]
pub fn workflow_operative(attr: TokenStream, item: TokenStream) -> TokenStream {
    on_run_fn(Declares::Operative(operative::Kind::Workflow), attr, item)
}

/// The three attributes on a `run` function share one expansion.
fn on_run_fn(declares: Declares, attr: TokenStream, item: TokenStream) -> TokenStream {
    procedure::expand(declares, attr.into(), item.into())
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
