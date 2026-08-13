//! `#[flezha::procedure(...)]` — attribute macro on a `run` function. Infers the
//! six `Procedure` associated types from the signature, generates the PascalCase
//! procedure type + `Procedure` impl, and (when `sink` is given) a single-procedure
//! auto-wired `StreamOperative` + inventory registration. See
//! `docs/sdk_authoring_contract.md` §2, §4, §5.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Expr, ExprLit, FnArg, ItemFn, Lit, MetaNameValue, Pat, ReturnType, Token, Type};

use crate::util::{first_generic_arg, snake_ident_to_pascal};

#[derive(Default)]
struct ProcedureArgs {
    id: Option<String>,
    sink: Option<String>,
    init: Option<syn::Path>,
    keys: Option<Vec<Expr>>,
    entities_for: Option<syn::Path>,
    idle_ttl_ms: Option<Expr>,
    max_entities: Option<Expr>,
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
                "#[flezha::procedure] `run` functions are free functions; `self` is not allowed",
            ));
        };
        match &*pt.ty {
            Type::Reference(r) if r.mutability.is_some() => {
                if state_param.is_some() {
                    return Err(syn::Error::new_spanned(
                        arg,
                        "#[flezha::procedure] expects at most one `&mut State` parameter",
                    ));
                }
                state_param = Some((&pt.pat, &r.elem));
            }
            Type::Reference(_) => {
                return Err(syn::Error::new_spanned(
                    arg,
                    "#[flezha::procedure] state parameter must be `&mut State`, not a shared reference",
                ));
            }
            other => {
                if input_param.is_some() {
                    return Err(syn::Error::new_spanned(
                        arg,
                        "#[flezha::procedure] expects exactly one by-value input parameter before the \
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
            "#[flezha::procedure] expects exactly one by-value input parameter",
        )
    })?;

    // --- Output inference from the return type. ---
    let output = infer_output(&sig.output)?;

    // --- Mode validation. Dynamic keying is declared on the wiring, so the
    // only mode this sugar can express is static. ---
    let static_mode = args.keys.is_some() || args.entities_for.is_some();
    if args.keys.is_some() != args.entities_for.is_some() {
        return Err(syn::Error::new(
            sig.span(),
            "#[flezha::procedure] static entity requires both `keys = [..]` and `entities_for = fn`",
        ));
    }
    if args.init.is_some() && state_param.is_none() {
        return Err(syn::Error::new(
            sig.span(),
            "#[flezha::procedure] `init` requires a `&mut State` parameter; a stateless procedure has no \
             state to initialize",
        ));
    }

    // --- Names. ---
    let struct_ident = snake_ident_to_pascal(fn_ident);
    let procedure_name = args.id.clone().unwrap_or_else(|| fn_ident.to_string());

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

    // --- init_state: `init = fn` maps the procedure key to the initial state
    // (per-entity config from the static entity key); otherwise the
    // state defaults. The payload is auto-applied by the runtime after this
    // returns, so the init fn never sees it.
    let init_state_fn = match (&state_param, &args.init) {
        (Some(_), Some(init_path)) => quote! {
            fn init_state(
                __flezha_entity_key: ::core::option::Option<&str>,
                _: ::core::option::Option<::flezha_runner::RestoreInitPayload<'_>>,
            ) -> Self::State {
                #init_path(__flezha_entity_key)
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
        quote! { __flezha_ctx: &mut ::flezha_runner::ProcedureCtx<'_> }
    } else {
        quote! { _flezha_ctx: &mut ::flezha_runner::ProcedureCtx<'_> }
    };

    // --- Optional Procedure-trait overrides. ---
    let entity_items = if static_mode {
        let keys = args.keys.as_ref().unwrap();
        let part = args.entities_for.as_ref().unwrap();
        quote! {
            fn entity_keys() -> ::std::vec::Vec<&'static str> { ::std::vec![ #(#keys),* ] }
            const ENTITIES_FOR_OVERRIDDEN: bool = true;
            fn entities_for(input: &Self::Input) -> ::std::vec::Vec<&'static str> {
                #part(input)
            }
        }
    } else {
        // Dynamic-mode knobs stay on the procedure — only the KEY moved to the
        // wiring — so they are emitted whenever declared.
        let ttl = args.idle_ttl_ms.as_ref().map(|ms| {
            quote! {
                fn entity_idle_ttl() -> ::core::option::Option<::core::time::Duration> {
                    ::core::option::Option::Some(::core::time::Duration::from_millis(#ms))
                }
            }
        });
        let maxp = args.max_entities.as_ref().map(|n| {
            quote! {
                fn max_entities() -> usize { #n }
                const MAX_ENTITIES_OVERRIDDEN: bool = true;
            }
        });
        quote! { #ttl #maxp }
    };

    let capacity_items = args.capacity.as_ref().map(|c| {
        quote! {
            fn mailbox_capacity() -> usize { #c }
            const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
        }
    });

    // --- Single-procedure auto-wire (sink present). ---
    let auto_operative = if let Some(sink) = &args.sink {
        let id = args.id.as_ref().ok_or_else(|| {
            syn::Error::new(
                sig.span(),
                "#[flezha::procedure] `sink` (single-procedure auto-wire) requires an `id`",
            )
        })?;
        if !output.emits() {
            return Err(syn::Error::new(
                sig.span(),
                "#[flezha::procedure] `sink` requires a non-unit return type to feed the plugin",
            ));
        }
        let operative_ident = format_ident!("__FlezhaAutoOperative{}", struct_ident);
        let on_full = on_full_call(&args.on_full)?;
        quote! {
            #[doc(hidden)]
            #vis struct #operative_ident;
            impl ::flezha_runner::StreamOperative for #operative_ident {
                fn factory_id() -> &'static str { #id }
                fn channels(c: &::flezha_runner::ChannelBuilder) {
                    let stream = c.input::<#input_ty>("input") #on_full .into::<#struct_ident>();
                    // Pins the sink payload type against the procedure's `Output`.
                    let stream: ::flezha_runner::Stream<'_, #output_ty_tokens> = stream;
                    stream.named(#sink).output(#sink);
                }
            }
            ::flezha_runner::inventory::submit! {
                ::flezha_runner::AutoOperative::new(
                    <#operative_ident as ::flezha_runner::StreamOperative>::factory_id,
                    || ::flezha_runner::__register_stream::<#operative_ident>(),
                )
            }
        }
    } else {
        quote! {}
    };

    Ok(quote! {
        #[derive(::core::default::Default)]
        #vis struct #struct_ident;

        impl ::flezha_runner::Procedure for #struct_ident {
            type Input = #input_ty;
            type Output = #output_ty_tokens;
            type State = #state_ty_tokens;
            type Change = #change_ty_tokens;
            type Clock = #clock_ty;
            type Config = ();

            fn name() -> &'static str { #procedure_name }

            #init_state_fn

            #apply_change_item
            #entity_items
            #capacity_items

            fn run(
                &mut self,
                #input_pat: Self::Input,
                #state_binding,
                #ctx_bind,
            ) -> ::core::result::Result<(), ::flezha_runner::ProcedureError> {
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
                "#[flezha::procedure] `on_full` must be one of: block, drop_newest, drop_oldest",
            )),
        };
    Ok(quote! { .on_full(::flezha_runner::OverflowPolicy::#variant) })
}

fn parse_args(attr: TokenStream) -> syn::Result<ProcedureArgs> {
    let mut args = ProcedureArgs::default();
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
            "entities_for" => args.entities_for = Some(expect_path(&value)?),
            "entity_key" => {
                return Err(syn::Error::new_spanned(
                    &meta.path,
                    "#[flezha::procedure]: `entity_key` no longer exists; declare the Entity key \
                     on the wiring instead — `.key_by(f)` on the stream that feeds this procedure",
                ))
            }
            "idle_ttl" => args.idle_ttl_ms = Some(value),
            "max_entities" => args.max_entities = Some(value),
            "capacity" => args.capacity = Some(value),
            "on_full" => args.on_full = Some(expect_ident(&value)?),
            "clock" => args.clock = Some(expect_type(&value)?),
            other => {
                return Err(syn::Error::new_spanned(
                    &meta.path,
                    format!("#[flezha::procedure]: unknown attribute `{other}`"),
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
            "expected a function path, e.g. `entities_for = my_fn`",
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
