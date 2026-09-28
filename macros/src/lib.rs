//! Proc-macros realizing the Flezha SDK authoring contract for Rust.
//!
//! - `#[flezha::tasklet]`: attribute on an `impl Tasklet for T` block. It
//!   generates no type; it completes the block with the items the author left
//!   out.
//! - `#[flezha::unbounded_operator]` / `#[flezha::bounded_operator]`: the §5
//!   short form. On a function it emits the operator type under the author's
//!   PascalCase name and the tasklet beside it, suffixed `Tasklet`; on an
//!   `impl Tasklet for T` block it completes the block and implements the
//!   operator trait on `T`.
//! - `flezha::operator!`: declarative macro lowering a strictly linear pipeline
//!   to a `channels()` body on the type it declares, which the author
//!   registers with `runner.register_factory(Name)`.
//!
//! Everything expands to the existing trait surface; see
//! `docs/sdk/authoring_contract.md`.

use proc_macro::TokenStream;

mod operator;
mod short_form;
mod tasklet;
mod util;

/// `#[flezha::tasklet(...)]` attribute macro. See module docs.
#[proc_macro_attribute]
pub fn tasklet(attr: TokenStream, item: TokenStream) -> TokenStream {
    into_output(tasklet::expand(attr.into(), item.into()))
}

/// `#[flezha::unbounded_operator(...)]` short-form attribute. See module docs.
#[proc_macro_attribute]
pub fn unbounded_operator(attr: TokenStream, item: TokenStream) -> TokenStream {
    into_output(short_form::expand(
        operator::Kind::Unbounded,
        attr.into(),
        item.into(),
    ))
}

/// `#[flezha::bounded_operator(...)]` short-form attribute. See module docs.
#[proc_macro_attribute]
pub fn bounded_operator(attr: TokenStream, item: TokenStream) -> TokenStream {
    into_output(short_form::expand(
        operator::Kind::Bounded,
        attr.into(),
        item.into(),
    ))
}

fn into_output(expanded: syn::Result<proc_macro2::TokenStream>) -> TokenStream {
    expanded
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// `flezha::operator! { id: "...", pipeline: ... }` declarative macro.
#[proc_macro]
pub fn operator(item: TokenStream) -> TokenStream {
    into_output(operator::expand(item.into()))
}
