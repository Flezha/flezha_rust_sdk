//! `#[flezha::shard(...)]` — attribute macro on a `run` function. Infers the
//! six `Shard` associated types from the signature, generates the PascalCase
//! shard type + `Shard` impl, and (when `sink` is given) a single-shard
//! auto-wired `Relay` + inventory registration. See
//! `docs/sdk_authoring_contract.md` §2, §4, §5.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Expr, ExprLit, FnArg, ItemFn, Lit, MetaNameValue, Pat, ReturnType, Token, Type};

use crate::util::{first_generic_arg, snake_ident_to_pascal};

#[derive(Default)]
struct ShardArgs {
    id: Option<String>,
    sink: Option<String>,
    keys: Option<Vec<Expr>>,
    partition_for: Option<syn::Path>,
    shard_key: Option<syn::Path>,
    idle_ttl_ms: Option<Expr>,
    max_partitions: Option<Expr>,
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
                "#[flezha::shard] `run` functions are free functions; `self` is not allowed",
            ));
        };
        match &*pt.ty {
            Type::Reference(r) if r.mutability.is_some() => {
                if state_param.is_some() {
                    return Err(syn::Error::new_spanned(
                        arg,
                        "#[flezha::shard] expects at most one `&mut State` parameter",
                    ));
                }
                state_param = Some((&pt.pat, &r.elem));
            }
            Type::Reference(_) => {
                return Err(syn::Error::new_spanned(
                    arg,
                    "#[flezha::shard] state parameter must be `&mut State`, not a shared reference",
                ));
            }
            other => {
                if input_param.is_some() {
                    return Err(syn::Error::new_spanned(
                        arg,
                        "#[flezha::shard] expects exactly one by-value input parameter before the \
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
            "#[flezha::shard] expects exactly one by-value input parameter",
        )
    })?;

    // --- Output inference from the return type. ---
    let output = infer_output(&sig.output)?;

    // --- Mode validation (static / dynamic mutually exclusive). ---
    let static_mode = args.keys.is_some() || args.partition_for.is_some();
    if static_mode && args.shard_key.is_some() {
        return Err(syn::Error::new(
            sig.span(),
            "#[flezha::shard] static keys (`keys`/`partition_for`) and dynamic `shard_key` are \
             mutually exclusive (composite mode is not supported)",
        ));
    }
    if args.keys.is_some() != args.partition_for.is_some() {
        return Err(syn::Error::new(
            sig.span(),
            "#[flezha::shard] static partitioning requires both `keys = [..]` and `partition_for = fn`",
        ));
    }

    // --- Names. ---
    let struct_ident = snake_ident_to_pascal(fn_ident);
    let shard_name = args.id.clone().unwrap_or_else(|| fn_ident.to_string());

    // --- State / Change wiring. ---
    let (state_ty_tokens, change_ty_tokens, init_state_expr, apply_change_item, state_binding, mut_use) =
        match &state_param {
            Some((pat, ty)) => {
                let handle_bind = quote! { mut #pat: ::flezha_executor::StateHandle<'_, Self> };
                let mut_use = mut_trait_use(ty);
                (
                    quote! { #ty },
                    quote! { <#ty as ::flezha_executor::DerivedState>::Change },
                    quote! { <#ty as ::core::default::Default>::default() },
                    quote! {
                        fn apply_change(state: &mut Self::State, change: &Self::Change) {
                            <#ty as ::flezha_executor::DerivedState>::apply_change(state, change);
                        }
                    },
                    handle_bind,
                    mut_use,
                )
            }
            None => (
                quote! { () },
                quote! { ::flezha_executor::NoChange },
                // Empty body: a `-> ()` fn returns unit without a bare `()` expr
                // (which clippy::unused_unit would flag).
                quote! {},
                quote! {},
                quote! { _: ::flezha_executor::StateHandle<'_, Self> },
                quote! {},
            ),
        };

    // --- Clock. ---
    let clock_ty = match &args.clock {
        Some(c) => quote! { #c },
        None => quote! { ::flezha_executor::WallClock },
    };

    // --- run body: bind input + state, run user body, emit the output. ---
    let user_body = &func.block;
    let (output_ty_tokens, run_emit) = match &output {
        Output::Unit => (quote! { () }, quote! { let _: () = #user_body; ::core::result::Result::Ok(()) }),
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
    };
    let ctx_bind = if output.emits() {
        quote! { __flezha_ctx: &mut ::flezha_executor::ShardCtx<'_> }
    } else {
        quote! { _flezha_ctx: &mut ::flezha_executor::ShardCtx<'_> }
    };

    // --- Optional Shard-trait overrides. ---
    let partition_items = if static_mode {
        let keys = args.keys.as_ref().unwrap();
        let part = args.partition_for.as_ref().unwrap();
        quote! {
            fn shard_keys() -> ::std::vec::Vec<&'static str> { ::std::vec![ #(#keys),* ] }
            const PARTITION_FOR_OVERRIDDEN: bool = true;
            fn partition_for(input: &Self::Input) -> ::std::vec::Vec<&'static str> {
                #part(input)
            }
        }
    } else if let Some(sk) = &args.shard_key {
        let ttl = args.idle_ttl_ms.as_ref().map(|ms| {
            quote! {
                fn partition_idle_ttl() -> ::core::option::Option<::core::time::Duration> {
                    ::core::option::Option::Some(::core::time::Duration::from_millis(#ms))
                }
            }
        });
        let maxp = args.max_partitions.as_ref().map(|n| {
            quote! {
                fn max_partitions() -> usize { #n }
                const MAX_PARTITIONS_OVERRIDDEN: bool = true;
            }
        });
        quote! {
            const SHARD_KEY_OVERRIDDEN: bool = true;
            fn shard_key(input: &Self::Input) -> ::core::option::Option<::std::string::String> {
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

    // --- Single-shard auto-wire (sink present). ---
    let auto_relay = if let Some(sink) = &args.sink {
        let id = args.id.as_ref().ok_or_else(|| {
            syn::Error::new(
                sig.span(),
                "#[flezha::shard] `sink` (single-shard auto-wire) requires an `id`",
            )
        })?;
        if !output.emits() {
            return Err(syn::Error::new(
                sig.span(),
                "#[flezha::shard] `sink` requires a non-unit return type to feed the plugin",
            ));
        }
        let relay_ident = format_ident!("__FlezhaAutoRelay{}", struct_ident);
        let on_full = on_full_call(&args.on_full)?;
        quote! {
            #[doc(hidden)]
            #vis struct #relay_ident;
            impl ::flezha_executor::Relay for #relay_ident {
                fn factory_id() -> &'static str { #id }
                fn channels(c: &mut ::flezha_executor::ChannelBuilder) {
                    c.input::<#input_ty>("input") #on_full .into::<#struct_ident>();
                    c.channel::<#output_ty_tokens>(#sink)
                        .from::<#struct_ident>()
                        .to_plugin(#sink);
                }
            }
            ::flezha_executor::inventory::submit! {
                ::flezha_executor::AutoRelay::new(
                    <#relay_ident as ::flezha_executor::Relay>::factory_id,
                    || ::flezha_executor::register_relay::<#relay_ident>(),
                )
            }
        }
    } else {
        quote! {}
    };

    Ok(quote! {
        #[derive(::core::default::Default)]
        #vis struct #struct_ident;

        impl ::flezha_executor::Shard for #struct_ident {
            type Input = #input_ty;
            type Output = #output_ty_tokens;
            type State = #state_ty_tokens;
            type Change = #change_ty_tokens;
            type Clock = #clock_ty;
            type Config = ();

            fn name() -> &'static str { #shard_name }

            fn init_state(
                _: ::core::option::Option<&str>,
                _: ::core::option::Option<::flezha_executor::RestoreInitPayload<'_>>,
            ) -> Self::State {
                #init_state_expr
            }

            #apply_change_item
            #partition_items
            #capacity_items

            fn run(
                &mut self,
                #input_pat: Self::Input,
                #state_binding,
                #ctx_bind,
            ) -> ::core::result::Result<(), ::flezha_executor::ShardError> {
                #[allow(unused_imports)]
                #mut_use
                #run_emit
            }
        }

        #auto_relay
    })
}

enum Output {
    Unit,
    ResultUnit,
    Value(Type),
    ResultValue(Type),
}

impl Output {
    fn emits(&self) -> bool {
        matches!(self, Output::Value(_) | Output::ResultValue(_))
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
        return Ok(if is_unit(inner) {
            Output::ResultUnit
        } else {
            Output::ResultValue(inner.clone())
        });
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
    let variant = match id.to_string().as_str() {
        "block" => quote! { Block },
        "drop_newest" => quote! { DropNewest },
        "drop_oldest" => quote! { DropOldest },
        _ => {
            return Err(syn::Error::new_spanned(
                id,
                "#[flezha::shard] `on_full` must be one of: block, drop_newest, drop_oldest",
            ))
        }
    };
    Ok(quote! { .on_full(::flezha_executor::OverflowPolicy::#variant) })
}

fn parse_args(attr: TokenStream) -> syn::Result<ShardArgs> {
    let mut args = ShardArgs::default();
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
            "keys" => args.keys = Some(expect_array(&value)?),
            "partition_for" => args.partition_for = Some(expect_path(&value)?),
            "shard_key" => args.shard_key = Some(expect_path(&value)?),
            "idle_ttl" => args.idle_ttl_ms = Some(value),
            "max_partitions" => args.max_partitions = Some(value),
            "capacity" => args.capacity = Some(value),
            "on_full" => args.on_full = Some(expect_ident(&value)?),
            "clock" => args.clock = Some(expect_type(&value)?),
            other => {
                return Err(syn::Error::new_spanned(
                    &meta.path,
                    format!("#[flezha::shard]: unknown attribute `{other}`"),
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
            "expected a function path, e.g. `partition_for = my_fn`",
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
