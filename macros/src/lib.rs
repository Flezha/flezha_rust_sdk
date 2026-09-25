//! Proc-macros realizing the Flezha SDK authoring contract for Rust.
//!
//! - `#[flezha::tasklet]` — attribute macro on a `run` function; infers the six
//!   `Tasklet` associated types from the signature and generates the impl and
//!   the PascalCase tasklet type.
//! - `#[flezha::unbounded_operator]` / `#[flezha::bounded_operator]`: the §5
//!   short form on a `run` function, emitting the operator type under the
//!   author's PascalCase name, which the author registers with
//!   `runner.register_factory(Name)`, and the tasklet type beside it, suffixed
//!   `Tasklet`.
//! - `#[derive(flezha::State)]` — generates a field-granular `Change` enum +
//!   `apply_change` + journaling mutators on the state handle.
//! - `flezha::operator!` — declarative macro lowering a strictly linear pipeline
//!   to a `channels()` body on the type it declares, which the author
//!   registers with `runner.register_factory(Name)`.
//!
//! Everything expands to the existing trait surface; see
//! `docs/sdk/authoring_contract.md`.

use proc_macro::TokenStream;

mod operator;
mod state;
mod tasklet;
mod util;

use tasklet::Declares;

/// `#[flezha::tasklet(...)]` attribute macro. See module docs.
#[proc_macro_attribute]
pub fn tasklet(attr: TokenStream, item: TokenStream) -> TokenStream {
    on_run_fn(Declares::Tasklet, attr, item)
}

/// `#[flezha::unbounded_operator(...)]` short-form attribute. See module docs.
#[proc_macro_attribute]
pub fn unbounded_operator(attr: TokenStream, item: TokenStream) -> TokenStream {
    on_run_fn(Declares::Operator(operator::Kind::Unbounded), attr, item)
}

/// `#[flezha::bounded_operator(...)]` short-form attribute. See module docs.
#[proc_macro_attribute]
pub fn bounded_operator(attr: TokenStream, item: TokenStream) -> TokenStream {
    on_run_fn(Declares::Operator(operator::Kind::Bounded), attr, item)
}

/// The three attributes on a `run` function share one expansion.
fn on_run_fn(declares: Declares, attr: TokenStream, item: TokenStream) -> TokenStream {
    tasklet::expand(declares, attr.into(), item.into())
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

/// `flezha::operator! { id: "...", pipeline: ... }` declarative macro.
#[proc_macro]
pub fn operator(item: TokenStream) -> TokenStream {
    operator::expand(item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
