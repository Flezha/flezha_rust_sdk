//! `#[flezha::operation(...)]` — attribute macro on a `run` function. Infers the
//! six `Operation` associated types from the signature, generates the PascalCase
//! operation type + `Operation` impl, and (when `sink` is given) a single-operation
//! auto-wired `UnboundedOperative` + inventory registration. See
//! `docs/sdk_authoring_contract.md` §2, §4, §5.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Expr, ExprLit, FnArg, ItemFn, Lit, MetaNameValue, Pat, ReturnType, Token, Type};

use crate::util::{first_generic_arg, snake_ident_to_pascal};

#[derive(Default)]
struct OperationArgs {
    id: Option<String>,
    sink: Option<String>,
    init: Option<syn::Path>,
    keys: Option<Vec<Expr>>,
    segment_for: Option<syn::Path>,
    operation_key: Option<syn::Path>,
    idle_ttl_ms: Option<Expr>,
    max_segments: Option<Expr>,
    capacity: Option<Expr>,
    on_full: Option<syn::Ident>,
    clock: Option<Type>,
}

pub fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let args = parse_args(attr)?;
    let func: ItemFn = syn::parse2(item)?;
    let sig = &func.sig;
    let fn_ident = &sig.ident;
    let vis = &func.vis;

    // --- Classify parameters: one by-value Input, optional `&mut State`. ---
    let mut input_param: Option<(&Pat, &Type)> = None;
    let mut state_param: Option<(&Pat, &Type)> = None;
    for arg in &sig.inputs {
        let FnArg::Typed(pt) = arg else {
            return Err(syn::Error::new_spanned(
                arg,
                "#[flezha::operation] `run` functions are free functions; `self` is not allowed",
            ));
        };
        match &*pt.ty {
            Type::Reference(r) if r.mutability.is_some() => {
                if state_param.is_some() {
                    return Err(syn::Error::new_spanned(
                        arg,
                        "#[flezha::operation] expects at most one `&mut State` parameter",
                    ));
                }
                state_param = Some((&pt.pat, &r.elem));
            }
            Type::Reference(_) => {
                return Err(syn::Error::new_spanned(
                    arg,
                    "#[flezha::operation] state parameter must be `&mut State`, not a shared reference",
                ));
            }
            other => {
                if input_param.is_some() {
                    return Err(syn::Error::new_spanned(
                        arg,
                        "#[flezha::operation] expects exactly one by-value input parameter before the \
                         optional `&mut State`",
                    ));
                }
                input_param = Some((&pt.pat, other));
            }
        }
    }
    let (input_pat, input_ty) = input_param.ok_or_else(|| {
        syn::Error::new(
            sig.span(),
            "#[flezha::operation] expects exactly one by-value input parameter",
        )
    })?;

    // --- Output inference from the return type. ---
    let output = infer_output(&sig.output)?;

    // --- Mode validation (static / dynamic mutually exclusive). ---
    let static_mode = args.keys.is_some() || args.segment_for.is_some();
    if static_mode && args.operation_key.is_some() {
        return Err(syn::Error::new(
            sig.span(),
            "#[flezha::operation] static keys (`keys`/`segment_for`) and dynamic `operation_key` are \
             mutually exclusive (composite mode is not supported)",
        ));
    }
    if args.keys.is_some() != args.segment_for.is_some() {
        return Err(syn::Error::new(
            sig.span(),
            "#[flezha::operation] static segmenting requires both `keys = [..]` and `segment_for = fn`",
        ));
    }
    if args.init.is_some() && state_param.is_none() {
        return Err(syn::Error::new(
            sig.span(),
            "#[flezha::operation] `init` requires a `&mut State` parameter; a stateless operation has no \
             state to initialize",
        ));
    }

    // --- Names. ---
    let struct_ident = snake_ident_to_pascal(fn_ident);
    let operation_name = args.id.clone().unwrap_or_else(|| fn_ident.to_string());

    // --- State / Change wiring. ---
    let (state_ty_tokens, change_ty_tokens, apply_change_item, state_binding, mut_use) =
        match &state_param {
            Some((pat, ty)) => {
                let handle_bind = quote! { mut #pat: ::flezha_runner::StateHandle<'_, Self> };
                let mut_use = mut_trait_use(ty);
                (
                    quote! { #ty },
                    quote! { <#ty as ::flezha_runner::DerivedState>::Change },
                    quote! {
                        fn apply_change(state: &mut Self::State, change: &Self::Change) {
                            <#ty as ::flezha_runner::DerivedState>::apply_change(state, change);
                        }
                    },
                    handle_bind,
                    mut_use,
                )
            }
            None => (
                quote! { () },
                quote! { ::flezha_runner::NoChange },
                quote! {},
                quote! { _: ::flezha_runner::StateHandle<'_, Self> },
                quote! {},
            ),
        };

    // --- init_state: `init = fn` maps the operation key to the initial state
    // (per-segment config from the static segment key); otherwise the
    // state defaults. The payload is auto-applied by the runtime after this
    // returns, so the init fn never sees it.
    let init_state_fn = match (&state_param, &args.init) {
        (Some(_), Some(init_path)) => quote! {
            fn init_state(
                __flezha_operation_key: ::core::option::Option<&str>,
                _: ::core::option::Option<::flezha_runner::RestoreInitPayload<'_>>,
            ) -> Self::State {
                #init_path(__flezha_operation_key)
            }
        },
        (Some((_, ty)), None) => quote! {
            fn init_state(
                _: ::core::option::Option<&str>,
                _: ::core::option::Option<::flezha_runner::RestoreInitPayload<'_>>,
            ) -> Self::State {
                <#ty as ::core::default::Default>::default()
            }
        },
        // Stateless: `-> ()` returns unit without a bare `()` expr (which
        // clippy::unused_unit would flag). `init` is rejected above.
        (None, _) => quote! {
            fn init_state(
                _: ::core::option::Option<&str>,
                _: ::core::option::Option<::flezha_runner::RestoreInitPayload<'_>>,
            ) -> Self::State {
            }
        },
    };

    // --- Clock. ---
    let clock_ty = match &args.clock {
        Some(c) => quote! { #c },
        None => quote! { ::flezha_runner::WallClock },
    };

    // --- run body: bind input + state, run user body, emit the output. ---
    let user_body = &func.block;
    let (output_ty_tokens, run_emit) = match &output {
        Output::Unit => (
            quote! { () },
            quote! { let _: () = #user_body; ::core::result::Result::Ok(()) },
        ),
        Output::ResultUnit => (
            quote! { () },
            quote! { let _: () = (#user_body)?; ::core::result::Result::Ok(()) },
        ),
        Output::Value(ty) => (
            quote! { #ty },
            quote! {
                let __flezha_out: Self::Output = #user_body;
                __flezha_ctx.emit::<Self::Output>(__flezha_out)
            },
        ),
        Output::ResultValue(ty) => (
            quote! { #ty },
            quote! {
                let __flezha_out: Self::Output = (#user_body)?;
                __flezha_ctx.emit::<Self::Output>(__flezha_out)
            },
        ),
        // The body is wrapped in a `move ||` so the author's `?` / `return None`
        // resolve against `Option<Output>`, not the `Result`-typed `run`.
        Output::OptionValue(ty) => (
            quote! { #ty },
            quote! {
                let __flezha_out: ::core::option::Option<Self::Output> = (move || #user_body)();
                match __flezha_out {
                    ::core::option::Option::Some(__v) => __flezha_ctx.emit::<Self::Output>(__v),
                    ::core::option::Option::None => ::core::result::Result::Ok(()),
                }
            },
        ),
        Output::ResultOptionValue(ty) => (
            quote! { #ty },
            quote! {
                let __flezha_out: ::core::option::Option<Self::Output> = (move || #user_body)()?;
                match __flezha_out {
                    ::core::option::Option::Some(__v) => __flezha_ctx.emit::<Self::Output>(__v),
                    ::core::option::Option::None => ::core::result::Result::Ok(()),
                }
            },
        ),
    };
    let ctx_bind = if output.emits() {
        quote! { __flezha_ctx: &mut ::flezha_runner::OperationCtx<'_> }
    } else {
        quote! { _flezha_ctx: &mut ::flezha_runner::OperationCtx<'_> }
    };

    // --- Optional Operation-trait overrides. ---
    let segment_items = if static_mode {
        let keys = args.keys.as_ref().unwrap();
        let part = args.segment_for.as_ref().unwrap();
        quote! {
            fn operation_keys() -> ::std::vec::Vec<&'static str> { ::std::vec![ #(#keys),* ] }
            const SEGMENT_FOR_OVERRIDDEN: bool = true;
            fn segment_for(input: &Self::Input) -> ::std::vec::Vec<&'static str> {
                #part(input)
            }
        }
    } else if let Some(sk) = &args.operation_key {
        let ttl = args.idle_ttl_ms.as_ref().map(|ms| {
            quote! {
                fn segment_idle_ttl() -> ::core::option::Option<::core::time::Duration> {
                    ::core::option::Option::Some(::core::time::Duration::from_millis(#ms))
                }
            }
        });
        let maxp = args.max_segments.as_ref().map(|n| {
            quote! {
                fn max_segments() -> usize { #n }
                const MAX_SEGMENTS_OVERRIDDEN: bool = true;
            }
        });
        quote! {
            const OPERATION_KEY_OVERRIDDEN: bool = true;
            fn operation_key(input: &Self::Input) -> ::core::option::Option<::std::string::String> {
                #sk(input)
            }
            #ttl
            #maxp
        }
    } else {
        quote! {}
    };

    let capacity_items = args.capacity.as_ref().map(|c| {
        quote! {
            fn mailbox_capacity() -> usize { #c }
            const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
        }
    });

    // --- Single-operation auto-wire (sink present). ---
    let auto_operative = if let Some(sink) = &args.sink {
        let id = args.id.as_ref().ok_or_else(|| {
            syn::Error::new(
                sig.span(),
                "#[flezha::operation] `sink` (single-operation auto-wire) requires an `id`",
            )
        })?;
        if !output.emits() {
            return Err(syn::Error::new(
                sig.span(),
                "#[flezha::operation] `sink` requires a non-unit return type to feed the plugin",
            ));
        }
        let operative_ident = format_ident!("__FlezhaAutoOperative{}", struct_ident);
        let on_full = on_full_call(&args.on_full)?;
        quote! {
            #[doc(hidden)]
            #vis struct #operative_ident;
            impl ::flezha_runner::UnboundedOperative for #operative_ident {
                fn factory_id() -> &'static str { #id }
                fn channels(c: &mut ::flezha_runner::ChannelBuilder) {
                    c.input::<#input_ty>("input") #on_full .into::<#struct_ident>();
                    c.channel::<#output_ty_tokens>(#sink)
                        .from::<#struct_ident>()
                        .to_plugin(#sink);
                }
            }
            ::flezha_runner::inventory::submit! {
                ::flezha_runner::AutoOperative::new(
                    <#operative_ident as ::flezha_runner::UnboundedOperative>::factory_id,
                    || ::flezha_runner::__register_unbounded::<#operative_ident>(),
                )
            }
        }
    } else {
        quote! {}
    };

    Ok(quote! {
        #[derive(::core::default::Default)]
        #vis struct #struct_ident;

        impl ::flezha_runner::Operation for #struct_ident {
            type Input = #input_ty;
            type Output = #output_ty_tokens;
            type State = #state_ty_tokens;
            type Change = #change_ty_tokens;
            type Clock = #clock_ty;
            type Config = ();

            fn name() -> &'static str { #operation_name }

            #init_state_fn

            #apply_change_item
            #segment_items
            #capacity_items

            fn run(
                &mut self,
                #input_pat: Self::Input,
                #state_binding,
                #ctx_bind,
            ) -> ::core::result::Result<(), ::flezha_runner::OperationError> {
                #[allow(unused_imports)]
                #mut_use
                #run_emit
            }
        }

        #auto_operative
    })
}

enum Output {
    Unit,
    ResultUnit,
    Value(Type),
    ResultValue(Type),
    OptionValue(Type),
    ResultOptionValue(Type),
}

impl Output {
    fn emits(&self) -> bool {
        matches!(
            self,
            Output::Value(_)
                | Output::ResultValue(_)
                | Output::OptionValue(_)
                | Output::ResultOptionValue(_)
        )
    }
}

fn infer_output(ret: &ReturnType) -> syn::Result<Output> {
    let ty = match ret {
        ReturnType::Default => return Ok(Output::Unit),
        ReturnType::Type(_, ty) => ty,
    };
    if is_unit(ty) {
        return Ok(Output::Unit);
    }
    if let Some(inner) = first_generic_arg(ty, "Result") {
        if is_unit(inner) {
            return Ok(Output::ResultUnit);
        }
        // `Result<Option<T>, _>` emits on `Some`, skips on `None`.
        if let Some(opt_inner) = first_generic_arg(inner, "Option") {
            return Ok(Output::ResultOptionValue(opt_inner.clone()));
        }
        return Ok(Output::ResultValue(inner.clone()));
    }
    // `Option<T>` emits on `Some`, skips on `None`.
    if let Some(opt_inner) = first_generic_arg(ty, "Option") {
        return Ok(Output::OptionValue(opt_inner.clone()));
    }
    Ok(Output::Value((**ty).clone()))
}

fn is_unit(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(t) if t.elems.is_empty())
}

/// `StateHandle` mutator-trait import for a derived state type `Foo` → bring
/// `FooMut` into scope so the user can call `state.push_field(..)`.
fn mut_trait_use(state_ty: &Type) -> TokenStream {
    let Type::Path(tp) = state_ty else {
        return quote! {};
    };
    let mut path = tp.path.clone();
    if let Some(last) = path.segments.last_mut() {
        last.ident = format_ident!("{}Mut", last.ident);
        last.arguments = syn::PathArguments::None;
    }
    quote! { use #path as _; }
}

fn on_full_call(on_full: &Option<syn::Ident>) -> syn::Result<TokenStream> {
    let Some(id) = on_full else {
        return Ok(quote! {});
    };
    let variant =
        match id.to_string().as_str() {
            "block" => quote! { Block },
            "drop_newest" => quote! { DropNewest },
            "drop_oldest" => quote! { DropOldest },
            _ => return Err(syn::Error::new_spanned(
                id,
                "#[flezha::operation] `on_full` must be one of: block, drop_newest, drop_oldest",
            )),
        };
    Ok(quote! { .on_full(::flezha_runner::OverflowPolicy::#variant) })
}

fn parse_args(attr: TokenStream) -> syn::Result<OperationArgs> {
    let mut args = OperationArgs::default();
    if attr.is_empty() {
        return Ok(args);
    }
    let metas = Punctuated::<MetaNameValue, Token![,]>::parse_terminated.parse2(attr)?;
    for meta in metas {
        let key = meta
            .path
            .get_ident()
            .ok_or_else(|| syn::Error::new_spanned(&meta.path, "expected a simple attribute key"))?
            .to_string();
        let value = meta.value;
        match key.as_str() {
            "id" => args.id = Some(expect_str(&value)?),
            "sink" => args.sink = Some(expect_str(&value)?),
            "init" => args.init = Some(expect_path(&value)?),
            "keys" => args.keys = Some(expect_array(&value)?),
            "segment_for" => args.segment_for = Some(expect_path(&value)?),
            "operation_key" => args.operation_key = Some(expect_path(&value)?),
            "idle_ttl" => args.idle_ttl_ms = Some(value),
            "max_segments" => args.max_segments = Some(value),
            "capacity" => args.capacity = Some(value),
            "on_full" => args.on_full = Some(expect_ident(&value)?),
            "clock" => args.clock = Some(expect_type(&value)?),
            other => {
                return Err(syn::Error::new_spanned(
                    &meta.path,
                    format!("#[flezha::operation]: unknown attribute `{other}`"),
                ))
            }
        }
    }
    Ok(args)
}

use syn::parse::Parser;

fn expect_str(expr: &Expr) -> syn::Result<String> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(s), ..
        }) => Ok(s.value()),
        _ => Err(syn::Error::new_spanned(expr, "expected a string literal")),
    }
}

fn expect_array(expr: &Expr) -> syn::Result<Vec<Expr>> {
    match expr {
        Expr::Array(arr) => Ok(arr.elems.iter().cloned().collect()),
        _ => Err(syn::Error::new_spanned(
            expr,
            "expected an array literal, e.g. `keys = [\"a\", \"b\"]`",
        )),
    }
}

fn expect_path(expr: &Expr) -> syn::Result<syn::Path> {
    match expr {
        Expr::Path(p) => Ok(p.path.clone()),
        _ => Err(syn::Error::new_spanned(
            expr,
            "expected a function path, e.g. `segment_for = my_fn`",
        )),
    }
}

fn expect_ident(expr: &Expr) -> syn::Result<syn::Ident> {
    match expr {
        Expr::Path(p) => p
            .path
            .get_ident()
            .cloned()
            .ok_or_else(|| syn::Error::new_spanned(expr, "expected a bare identifier")),
        _ => Err(syn::Error::new_spanned(expr, "expected a bare identifier")),
    }
}

fn expect_type(expr: &Expr) -> syn::Result<Type> {
    match expr {
        Expr::Path(p) => Ok(Type::Path(syn::TypePath {
            qself: p.qself.clone(),
            path: p.path.clone(),
        })),
        _ => Err(syn::Error::new_spanned(expr, "expected a type path")),
    }
}
