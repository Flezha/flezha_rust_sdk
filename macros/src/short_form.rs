//! `#[flezha::unbounded_operator]` / `#[flezha::bounded_operator]`: a whole
//! one-tasklet operator, on one of two targets.
//!
//! - A function with no state parameter, returning its output: the attribute
//!   emits the operator type under the author's PascalCase name and, beside it,
//!   the tasklet under that name suffixed `Tasklet`, an `impl Tasklet` block
//!   completed the way `#[flezha::tasklet]` completes one.
//! - An `impl Tasklet for X` block: the attribute completes the block, naming
//!   the tasklet after the `id`, and implements the operator trait on `X`
//!   itself, generating no type.
//!
//! See `docs/sdk/authoring_contract.md` §5.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{FnArg, ImplItem, Item, ItemFn, ItemImpl, LitStr, Meta, Pat, ReturnType, Type};

use crate::operator::{operator_item, Kind, Wiring};
use crate::tasklet::{
    complete, impl_target, key_of, lit_str, parse_metas, unknown_key, TaskletKeys, OPERATOR_KEYS,
};
use crate::util::{generic_args, snake_ident_to_pascal};

/// The operator keys, beside the tasklet keys both targets share.
#[derive(Default)]
struct OperatorKeys {
    id: Option<LitStr>,
    /// The plugin sink; `None` declares no plugin channel.
    sink: Option<String>,
    /// The entry channel name.
    input: Option<LitStr>,
    /// The entry channel's `.on_full(..)` call, empty when none was written.
    on_full: TokenStream,
    /// A bounded operator's `loop_mode()` item, empty when none was written.
    loop_mode: TokenStream,
}

pub(crate) fn expand(kind: Kind, attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let (keys, tasklet) = parse_keys(kind, attr)?;
    let item: Item = syn::parse2(item)?;
    match item {
        Item::Fn(func) => on_function(kind, &keys, &tasklet, func),
        other => on_impl(kind, &keys, &tasklet, other),
    }
}

fn parse_keys(kind: Kind, attr: TokenStream) -> syn::Result<(OperatorKeys, TaskletKeys)> {
    let attribute = kind.attribute();
    let mut keys = OperatorKeys::default();
    let mut tasklet = TaskletKeys::default();
    for meta in parse_metas(attr)? {
        let key = key_of(&meta)?;
        let value = || meta.require_name_value().map(|nv| &nv.value);
        match key.as_str() {
            "id" => keys.id = Some(lit_str(value()?)?.clone()),
            "sink" => keys.sink = Some(lit_str(value()?)?.value()),
            "input" => keys.input = Some(lit_str(value()?)?.clone()),
            "on_full" => keys.on_full = on_full_call(attribute, &ident(value()?)?)?,
            "loop_mode" if matches!(kind, Kind::Unbounded) => {
                return Err(syn::Error::new_spanned(
                    meta.path(),
                    "#[flezha::unbounded_operator]: `loop_mode` is a bounded setting; declare the \
                     short form with `#[flezha::bounded_operator(..)]`",
                ))
            }
            "loop_mode" => keys.loop_mode = loop_mode_item(lit_str(value()?)?)?,
            _ if tasklet.take(&key, &meta)? => {}
            _ => return Err(refused_key(&meta, &key, attribute)),
        }
    }
    Ok((keys, tasklet))
}

fn refused_key(meta: &Meta, key: &str, attribute: &str) -> syn::Error {
    if key == "name" {
        return syn::Error::new_spanned(
            meta.path(),
            format!("{attribute}: the tasklet is named after the `id`; remove `name`"),
        );
    }
    unknown_key(meta, key, attribute, &OPERATOR_KEYS)
}

fn ident(expr: &syn::Expr) -> syn::Result<syn::Ident> {
    let ident = match expr {
        syn::Expr::Path(p) => p.path.get_ident().cloned(),
        _ => None,
    };
    ident.ok_or_else(|| syn::Error::new_spanned(expr, "expected a bare identifier"))
}

/// The `id` every operator needs: a factory id is an address a user types.
fn required_id(kind: Kind, keys: &OperatorKeys, span: proc_macro2::Span) -> syn::Result<LitStr> {
    keys.id.clone().ok_or_else(|| {
        syn::Error::new(
            span,
            format!(
                "{} needs an `id`: an operator's factory id is an address a user types, \
                 so it is written rather than derived from the function name",
                kind.attribute()
            ),
        )
    })
}

fn sink_without_output(span: proc_macro2::Span) -> syn::Error {
    syn::Error::new(
        span,
        "SinkWithoutOutput: a `sink` requires an output to feed the plugin: a non-unit \
         return, or a written `type Output`",
    )
}

/// The operator wiring the entry channel into `tasklet`, and `tasklet` to the
/// sink when one is written.
fn wiring<'a>(
    kind: Kind,
    keys: &'a OperatorKeys,
    id: &'a LitStr,
    tasklet: &Type,
    vis: Option<&'a syn::Visibility>,
    ident: &'a syn::Ident,
) -> syn::Result<TokenStream> {
    let id = id.value();
    operator_item(Wiring {
        vis,
        ident,
        kind,
        id: &id,
        input_ty: &syn::parse_quote! { <#tasklet as ::flezha_runner::Tasklet>::Input },
        input_name: keys.input.as_ref(),
        on_full: keys.on_full.clone(),
        chain: quote! { .into::<#tasklet>(); },
        payload: quote! { <#tasklet as ::flezha_runner::Tasklet>::Output },
        sink: keys.sink.as_deref(),
        settings: keys.loop_mode.clone(),
    })
}

/// The `impl Tasklet for X` target: the block completed under the `id`, and
/// the operator trait on `X` itself.
fn on_impl(
    kind: Kind,
    keys: &OperatorKeys,
    tasklet: &TaskletKeys,
    item: Item,
) -> syn::Result<TokenStream> {
    let block: ItemImpl = impl_target(quote! { #item }, kind.attribute())?;
    let id = required_id(kind, keys, block.self_ty.span())?;
    if keys.sink.is_some() && !writes_output(&block) {
        return Err(sink_without_output(block.self_ty.span()));
    }
    let self_ty = (*block.self_ty).clone();
    let Type::Path(path) = &self_ty else {
        return Err(syn::Error::new_spanned(
            &self_ty,
            format!("{} expects a named type", kind.attribute()),
        ));
    };
    let ident = &path
        .path
        .segments
        .last()
        .expect("a type path has a segment")
        .ident;
    let operator = wiring(kind, keys, &id, &self_ty, None, ident)?;
    let completed = complete(tasklet, &id, block)?;
    Ok(quote! {
        #completed
        #operator
    })
}

/// Whether the block writes a non-unit `type Output`.
fn writes_output(block: &ItemImpl) -> bool {
    block
        .items
        .iter()
        .any(|item| matches!(item, ImplItem::Type(t) if t.ident == "Output" && !is_unit(&t.ty)))
}

/// The function target: the tasklet `XTasklet`, whose `run` emits what the
/// function returns, and the operator `X` wiring it.
fn on_function(
    kind: Kind,
    keys: &OperatorKeys,
    tasklet: &TaskletKeys,
    func: ItemFn,
) -> syn::Result<TokenStream> {
    let attribute = kind.attribute();
    let (input_pat, input_ty) = input_param(&func, attribute)?;
    let output = resolve_output(&func.sig.output);
    let id = required_id(kind, keys, func.sig.span())?;
    if keys.sink.is_some() && !output.emits() {
        return Err(sink_without_output(func.sig.span()));
    }
    let operator_ident = snake_ident_to_pascal(&func.sig.ident);
    let tasklet_ident = format_ident!("{}Tasklet", operator_ident);
    let output_ty = match &output.shape {
        Shape::Value(ty) | Shape::OptionValue(ty) => quote! { #ty },
        Shape::Unit => quote! { () },
    };
    let body = run_body(&func, &output);
    let ctx = if output.emits() {
        quote! { __flezha_ctx }
    } else {
        quote! { _ }
    };
    let block: ItemImpl = syn::parse_quote! {
        impl ::flezha_runner::Tasklet for #tasklet_ident {
            type Input = #input_ty;
            type Output = #output_ty;
            fn run(
                &mut self,
                #input_pat: Self::Input,
                #ctx: &mut ::flezha_runner::Ctx<'_, Self>,
            ) -> ::core::result::Result<(), ::flezha_runner::TaskletError> {
                #body
            }
        }
    };
    let completed = complete(tasklet, &id, block)?;
    let tasklet_ty: Type = syn::parse_quote! { #tasklet_ident };
    let vis = &func.vis;
    let operator = wiring(kind, keys, &id, &tasklet_ty, Some(vis), &operator_ident)?;
    Ok(quote! {
        #[derive(::core::default::Default)]
        #vis struct #tasklet_ident;
        #completed
        #operator
    })
}

/// The one by-value input a function short form takes. A `&mut` parameter
/// is state or a context, which the `impl` target carries.
fn input_param<'a>(func: &'a ItemFn, attribute: &str) -> syn::Result<(&'a Pat, &'a Type)> {
    let mut input = None;
    for arg in &func.sig.inputs {
        let FnArg::Typed(pt) = arg else {
            return Err(syn::Error::new_spanned(
                arg,
                format!("{attribute} functions are free functions; `self` is not allowed"),
            ));
        };
        match &*pt.ty {
            Type::Reference(r) if r.mutability.is_some() => {
                return Err(syn::Error::new_spanned(
                    pt,
                    format!(
                        "ShortFormStateOnFunction: {attribute} on a function takes one input and \
                         returns its output; a tasklet with state or a context is written as \
                         `impl Tasklet for YourType` under the same attribute"
                    ),
                ))
            }
            Type::Reference(_) => {
                return Err(syn::Error::new_spanned(
                    pt,
                    format!("{attribute} takes its input by value, not by reference"),
                ))
            }
            _ if input.is_some() => {
                return Err(syn::Error::new_spanned(
                    arg,
                    format!("{attribute} expects exactly one by-value input parameter"),
                ))
            }
            ty => input = Some((&*pt.pat, ty)),
        }
    }
    input.ok_or_else(|| {
        syn::Error::new(
            func.sig.span(),
            format!("{attribute} expects exactly one by-value input parameter"),
        )
    })
}

/// What the return type says about the emit, on its two independent axes: the
/// shape of the value, and whether the author wrapped it in a `Result`.
struct Output {
    shape: Shape,
    result: bool,
}

enum Shape {
    Unit,
    Value(Type),
    /// Emits on `Some`, skips on `None`.
    OptionValue(Type),
}

impl Output {
    fn emits(&self) -> bool {
        !matches!(self.shape, Shape::Unit)
    }
}

fn resolve_output(ret: &ReturnType) -> Output {
    let ReturnType::Type(_, ty) = ret else {
        return Output {
            shape: Shape::Unit,
            result: false,
        };
    };
    let (inner, result) = match generic_args(ty, "Result").next() {
        Some(inner) => (inner, true),
        None => (&**ty, false),
    };
    let shape = if is_unit(inner) {
        Shape::Unit
    } else if let Some(opt_inner) = generic_args(inner, "Option").next() {
        Shape::OptionValue(opt_inner.clone())
    } else {
        Shape::Value(inner.clone())
    };
    Output { shape, result }
}

fn is_unit(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(t) if t.elems.is_empty())
}

/// The generated `run` body: the author's block, then the emit it returns.
fn run_body(func: &ItemFn, output: &Output) -> TokenStream {
    let user_body = &func.block;
    // The `Result` arms annotate the body with the return type the author
    // declared, so `?` inside it resolves against that error type instead of
    // being inferred from the generated `run`'s.
    let declared_ret = match &func.sig.output {
        ReturnType::Type(_, ty) => quote! { #ty },
        ReturnType::Default => quote! { () },
    };
    // An `Option` body is wrapped in a `move ||` so the author's `?` /
    // `return None` resolve against `Option<Output>`, not the `Result`-typed
    // generated `run`.
    let bound_body = match &output.shape {
        Shape::OptionValue(_) => quote! { (move || #user_body)() },
        _ => quote! { #user_body },
    };
    let value = if output.result {
        quote! {{ let __flezha_res: #declared_ret = #bound_body; __flezha_res? }}
    } else {
        bound_body
    };
    match &output.shape {
        Shape::Unit => quote! { let _: () = #value; ::core::result::Result::Ok(()) },
        Shape::Value(_) => quote! {
            let __flezha_out: Self::Output = #value;
            __flezha_ctx.emit(__flezha_out)
        },
        Shape::OptionValue(_) => quote! {
            let __flezha_opt: ::core::option::Option<Self::Output> = #value;
            match __flezha_opt {
                ::core::option::Option::Some(__flezha_out) => __flezha_ctx.emit(__flezha_out),
                ::core::option::Option::None => ::core::result::Result::Ok(()),
            }
        },
    }
}

/// The variant of `ty` that `value` names among `choices`, refused with
/// `refusal` when it names none.
fn variant(
    value: &str,
    spanned: impl quote::ToTokens,
    choices: &[(&str, &str)],
    ty: TokenStream,
    refusal: String,
) -> syn::Result<TokenStream> {
    let (_, variant) = choices
        .iter()
        .find(|(written, _)| *written == value)
        .ok_or_else(|| syn::Error::new_spanned(spanned, refusal))?;
    let variant = syn::Ident::new(variant, proc_macro2::Span::call_site());
    Ok(quote! { #ty::#variant })
}

/// A bounded operator's `loop_mode()`.
fn loop_mode_item(lit: &LitStr) -> syn::Result<TokenStream> {
    let mode = variant(
        &lit.value(),
        lit,
        &[
            ("once", "Once"),
            ("per_input", "PerInput"),
            ("chained", "Chained"),
        ],
        quote! { ::flezha_runner::bounded::LoopMode },
        "#[flezha::bounded_operator] `loop_mode` must be one of: \"once\", \"per_input\", \
         \"chained\""
            .to_string(),
    )?;
    Ok(quote! {
        fn loop_mode() -> ::flezha_runner::bounded::LoopMode { #mode }
    })
}

/// The entry channel's `.on_full(..)` call.
fn on_full_call(attr: &str, id: &syn::Ident) -> syn::Result<TokenStream> {
    let policy = variant(
        &id.to_string(),
        id,
        &[
            ("drop_nothing", "DropNothing"),
            ("drop_newest", "DropNewest"),
            ("drop_oldest", "DropOldest"),
        ],
        quote! { ::flezha_runner::OverflowPolicy },
        format!("{attr} `on_full` must be one of: drop_nothing, drop_newest, drop_oldest"),
    )?;
    Ok(quote! { .on_full(#policy) })
}
