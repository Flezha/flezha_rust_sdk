//! `#[flezha::procedure(...)]` — attribute macro on a `run` function. Infers the
//! six `Procedure` associated types from the signature, generates the PascalCase
//! procedure type + `Procedure` impl, and (when `sink` is given) a single-procedure
//! auto-wired `StreamOperative` + inventory registration. See
//! `docs/sdk/authoring_contract.md` §2, §4, §5.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{
    Expr, ExprLit, FnArg, ItemFn, Lit, LitStr, MetaNameValue, Pat, PatType, ReturnType, Signature,
    Token, Type,
};

use crate::operative::{auto_operative, AutoWiring};
use crate::util::{generic_args, snake_ident_to_pascal};

#[derive(Default)]
struct ProcedureArgs {
    id: Option<String>,
    sink: Option<String>,
    /// The entry channel name for the auto-wired operative a `sink` generates.
    input: Option<LitStr>,
    init: Option<syn::Path>,
    key: Option<syn::Path>,
    emits: Option<Type>,
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
    let params = classify_params(&func.sig)?;
    let output = resolve_output(&func.sig.output)?;
    refuse_shape(&func.sig, &args, &output, &params)?;
    expand_procedure(&func, &args, &params, &output)
}

/// The three roles a `run` parameter can take: one by-value input, an optional
/// `&mut State`, and an optional trailing `&mut ProcedureCtx`.
struct Params<'a> {
    input_pat: &'a Pat,
    input_ty: &'a Type,
    state: Option<(&'a Pat, &'a Type)>,
    ctx: Option<&'a Pat>,
}

enum Role<'a> {
    Input(&'a Pat, &'a Type),
    State(&'a Pat, &'a Type),
    Ctx(&'a Pat),
}

fn classify_params(sig: &Signature) -> syn::Result<Params<'_>> {
    let mut input: Option<(&Pat, &Type)> = None;
    let mut state: Option<(&Pat, &Type)> = None;
    let mut ctx: Option<&Pat> = None;
    for arg in &sig.inputs {
        let FnArg::Typed(pt) = arg else {
            return Err(syn::Error::new_spanned(
                arg,
                "#[flezha::procedure] `run` functions are free functions; `self` is not allowed",
            ));
        };
        if ctx.is_some() {
            return Err(syn::Error::new_spanned(
                arg,
                "#[flezha::procedure] the `&mut ProcedureCtx` parameter must come last",
            ));
        }
        match classify_arg(pt)? {
            Role::Ctx(pat) => ctx = Some(pat),
            Role::State(pat, ty) => claim_once(
                &mut state,
                (pat, ty),
                arg,
                "#[flezha::procedure] expects at most one `&mut State` parameter",
            )?,
            Role::Input(pat, ty) => claim_once(
                &mut input,
                (pat, ty),
                arg,
                "#[flezha::procedure] expects exactly one by-value input parameter before the \
                 optional `&mut State`",
            )?,
        }
    }
    let (input_pat, input_ty) = input.ok_or_else(|| {
        syn::Error::new(
            sig.span(),
            "#[flezha::procedure] expects exactly one by-value input parameter",
        )
    })?;
    Ok(Params {
        input_pat,
        input_ty,
        state,
        ctx,
    })
}

/// A role that admits one parameter takes the first and refuses the second.
fn claim_once<'a>(
    slot: &mut Option<(&'a Pat, &'a Type)>,
    param: (&'a Pat, &'a Type),
    arg: &FnArg,
    refusal: &str,
) -> syn::Result<()> {
    if slot.is_some() {
        return Err(syn::Error::new_spanned(arg, refusal));
    }
    *slot = Some(param);
    Ok(())
}

fn classify_arg(pt: &PatType) -> syn::Result<Role<'_>> {
    Ok(match &*pt.ty {
        Type::Reference(r) if r.mutability.is_some() && is_procedure_ctx(&r.elem) => {
            Role::Ctx(&pt.pat)
        }
        Type::Reference(r) if r.mutability.is_some() => Role::State(&pt.pat, &r.elem),
        Type::Reference(_) => {
            return Err(syn::Error::new_spanned(
                pt,
                "#[flezha::procedure] state parameter must be `&mut State`, not a shared reference",
            ))
        }
        other => Role::Input(&pt.pat, other),
    })
}

/// The shapes the sugar cannot express: a context parameter that also returns a
/// value or keys its emit, `emits` without that parameter, a half-declared
/// static entity, and `init` or `key` with nothing to apply them to.
fn refuse_shape(
    sig: &Signature,
    args: &ProcedureArgs,
    output: &Output,
    params: &Params,
) -> syn::Result<()> {
    if params.ctx.is_some() {
        if output.emits() {
            return Err(syn::Error::new(
                sig.span(),
                "#[flezha::procedure] a `&mut ProcedureCtx` parameter makes the body responsible for every \
                 emit; `run` must then return `()` or `Result<(), _>`, and `emits = Type` declares the \
                 output type",
            ));
        }
        if args.key.is_some() {
            return Err(syn::Error::new(
                sig.span(),
                "#[flezha::procedure] `key` conflicts with a `&mut ProcedureCtx` parameter; the body keys \
                 its own emits with `ctx.emit_keyed(..)`",
            ));
        }
    } else if args.emits.is_some() {
        return Err(syn::Error::new(
            sig.span(),
            "#[flezha::procedure] `emits` declares the output type of a `&mut ProcedureCtx` `run`; without \
             that parameter the return type declares it",
        ));
    }
    if args.keys.is_some() != args.entities_for.is_some() {
        return Err(syn::Error::new(
            sig.span(),
            "#[flezha::procedure] static entity requires both `keys = [..]` and `entities_for = fn`",
        ));
    }
    if args.init.is_some() && params.state.is_none() {
        return Err(syn::Error::new(
            sig.span(),
            "#[flezha::procedure] `init` requires a `&mut State` parameter; a stateless procedure has no \
             state to initialize",
        ));
    }
    if args.key.is_some() && !output.emits() {
        return Err(syn::Error::new(
            sig.span(),
            "#[flezha::procedure] `key` requires a non-unit return type; there is no emit to key",
        ));
    }
    Ok(())
}

fn expand_procedure(
    func: &ItemFn,
    args: &ProcedureArgs,
    params: &Params,
    output: &Output,
) -> syn::Result<TokenStream> {
    let sig = &func.sig;
    let vis = &func.vis;
    let struct_ident = snake_ident_to_pascal(&sig.ident);
    let procedure_name = args.id.clone().unwrap_or_else(|| sig.ident.to_string());
    let state = expand_state(params.state, &args.init);
    let (state_types, state_fns) = (&state.types, &state.fns);
    let clock_ty = resolve_clock_ty(&args.clock);
    let output_ty = resolve_output_ty(args, output);
    let run_item = expand_run(func, params, output, &args.key, &state);
    let overrides = expand_modes(args);
    let auto_operative = expand_auto_wire(func, args, params, output, &output_ty, &struct_ident)?;
    let input_ty = params.input_ty;
    Ok(quote! {
        #[derive(::core::default::Default)]
        #vis struct #struct_ident;

        impl ::flezha_runner::Procedure for #struct_ident {
            type Input = #input_ty;
            type Output = #output_ty;
            #state_types
            type Clock = #clock_ty;
            type Config = ();

            fn name() -> &'static str { #procedure_name }

            #state_fns
            #overrides
            #run_item
        }

        #auto_operative
    })
}

/// Everything the `&mut State` parameter decides: the two associated types, the
/// `init_state` and `apply_change` items, the `run` binding, and the mutator
/// trait the body needs in scope.
struct StateItems {
    types: TokenStream,
    fns: TokenStream,
    binding: TokenStream,
    mut_use: TokenStream,
}

fn expand_state(state: Option<(&Pat, &Type)>, init: &Option<syn::Path>) -> StateItems {
    let fns = expand_init_state(state, init);
    match state {
        Some((pat, ty)) => StateItems {
            types: quote! {
                type State = #ty;
                type Change = <#ty as ::flezha_runner::DerivedState>::Change;
            },
            fns: quote! {
                #fns
                fn apply_change(state: &mut Self::State, change: &Self::Change) {
                    <#ty as ::flezha_runner::DerivedState>::apply_change(state, change);
                }
            },
            binding: quote! { mut #pat: ::flezha_runner::StateHandle<'_, Self> },
            mut_use: mut_trait_use(ty),
        },
        None => StateItems {
            types: quote! {
                type State = ();
                type Change = ::flezha_runner::NoChange;
            },
            fns,
            binding: quote! { _: ::flezha_runner::StateHandle<'_, Self> },
            mut_use: quote! {},
        },
    }
}

/// `init = fn` maps the procedure key to the initial state (per-entity config
/// from the static entity key); otherwise the state defaults. The payload is
/// auto-applied by the runtime after this returns, so the init fn never sees it.
fn expand_init_state(state: Option<(&Pat, &Type)>, init: &Option<syn::Path>) -> TokenStream {
    match (state, init) {
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
        // clippy::unused_unit would flag). `init` is refused above.
        (None, _) => quote! {
            fn init_state(
                _: ::core::option::Option<&str>,
                _: ::core::option::Option<::flezha_runner::RestoreInitPayload<'_>>,
            ) -> Self::State {
            }
        },
    }
}

fn resolve_clock_ty(clock: &Option<Type>) -> TokenStream {
    match clock {
        Some(c) => quote! { #c },
        None => quote! { ::flezha_runner::WallClock },
    }
}

fn resolve_output_ty(args: &ProcedureArgs, output: &Output) -> TokenStream {
    match (&args.emits, &output.shape) {
        (Some(emits), _) => quote! { #emits },
        (None, Shape::Value(ty) | Shape::OptionValue(ty)) => quote! { #ty },
        (None, Shape::Unit) => quote! { () },
    }
}

fn resolve_ctx_binding(ctx: Option<&Pat>, output: &Output) -> TokenStream {
    match (ctx, output.emits()) {
        (Some(pat), _) => quote! { #pat: &mut ::flezha_runner::ProcedureCtx<'_> },
        (None, true) => quote! { __flezha_ctx: &mut ::flezha_runner::ProcedureCtx<'_> },
        (None, false) => quote! { _flezha_ctx: &mut ::flezha_runner::ProcedureCtx<'_> },
    }
}

/// The generated `run`: the parameter bindings the signature and the state
/// decided, then the body.
fn expand_run(
    func: &ItemFn,
    params: &Params,
    output: &Output,
    key: &Option<syn::Path>,
    state: &StateItems,
) -> TokenStream {
    let input_pat = params.input_pat;
    let (state_binding, mut_use) = (&state.binding, &state.mut_use);
    let ctx_bind = resolve_ctx_binding(params.ctx, output);
    let body = expand_run_body(func, output, key);
    quote! {
        fn run(
            &mut self,
            #input_pat: Self::Input,
            #state_binding,
            #ctx_bind,
        ) -> ::core::result::Result<(), ::flezha_runner::ProcedureError> {
            #[allow(unused_imports)]
            #mut_use
            #body
        }
    }
}

/// The `run` body: bind the input, run the user block, emit the output.
fn expand_run_body(func: &ItemFn, output: &Output, key: &Option<syn::Path>) -> TokenStream {
    let user_body = &func.block;
    // The `Result` arms annotate the body with the return type the author
    // declared, so `?` inside it resolves against that error type instead of
    // being inferred from the generated `run`'s.
    let declared_ret = match &func.sig.output {
        ReturnType::Type(_, ty) => quote! { #ty },
        ReturnType::Default => quote! { () },
    };
    let emit_out = expand_emit(key);
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
            #emit_out
        },
        Shape::OptionValue(_) => quote! {
            let __flezha_opt: ::core::option::Option<Self::Output> = #value;
            match __flezha_opt {
                ::core::option::Option::Some(__flezha_out) => #emit_out,
                ::core::option::Option::None => ::core::result::Result::Ok(()),
            }
        },
    }
}

/// `key = fn` (a `Fn(&Output) -> String`) routes the generated emit through
/// `emit_keyed`, the only form a `.keyed()` output channel accepts. Without it
/// the sugar could never feed a keyed sink and every such procedure would have
/// to be hand-written.
fn expand_emit(key: &Option<syn::Path>) -> TokenStream {
    match key {
        Some(key_path) => quote! {{
            let __flezha_key: ::std::string::String = #key_path(&__flezha_out);
            __flezha_ctx.emit_keyed::<Self::Output>(__flezha_key, __flezha_out)
        }},
        None => quote! { __flezha_ctx.emit::<Self::Output>(__flezha_out) },
    }
}

/// The entity-mode and mailbox overrides. Dynamic keying is declared on the
/// wiring, so the only mode this sugar can express is static; the dynamic knobs
/// stay on the procedure and are emitted whenever declared.
fn expand_modes(args: &ProcedureArgs) -> TokenStream {
    let entity_items = match (&args.keys, &args.entities_for) {
        (Some(keys), Some(part)) => quote! {
            fn entity_keys() -> ::std::vec::Vec<&'static str> { ::std::vec![ #(#keys),* ] }
            const ENTITIES_FOR_OVERRIDDEN: bool = true;
            fn entities_for(input: &Self::Input) -> ::std::vec::Vec<&'static str> {
                #part(input)
            }
        },
        _ => {
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
        }
    };
    let capacity_items = args.capacity.as_ref().map(|c| {
        quote! {
            fn mailbox_capacity() -> usize { #c }
            const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
        }
    });
    quote! { #entity_items #capacity_items }
}

/// Every rule about what a `sink` needs and what needs a `sink`, resolved to
/// the `(id, sink)` pair the auto-wire emits from. `None` when the class
/// declares no `sink` and nothing asked for one.
fn auto_wire_names<'a>(
    sig: &Signature,
    args: &'a ProcedureArgs,
    output: &Output,
) -> syn::Result<Option<(&'a str, &'a str)>> {
    let Some(sink) = &args.sink else {
        // Nothing is auto-wired, so an entry channel name has no channel to
        // name and would sit in the attribute doing nothing.
        return match &args.input {
            Some(name) => Err(syn::Error::new_spanned(
                name,
                format!(
                    "#[flezha::procedure] `input = {:?}` names the entry channel of the operative \
                     a `sink` auto-wires; this procedure declares no `sink`, so there is no \
                     channel to name",
                    name.value()
                ),
            )),
            None => Ok(None),
        };
    };
    let id = args.id.as_deref().ok_or_else(|| {
        syn::Error::new(
            sig.span(),
            "#[flezha::procedure] `sink` (single-procedure auto-wire) requires an `id`",
        )
    })?;
    if !(output.emits() || args.emits.is_some()) {
        return Err(syn::Error::new(
            sig.span(),
            "#[flezha::procedure] `sink` requires an output type to feed the plugin — a non-unit \
             return, or `emits = Type` on the context-parameter shape",
        ));
    }
    Ok(Some((id, sink)))
}

/// `sink = "name"` wires this one procedure into its own `StreamOperative` and
/// submits it to the inventory, so a single-procedure class needs no builder.
fn expand_auto_wire(
    func: &ItemFn,
    args: &ProcedureArgs,
    params: &Params,
    output: &Output,
    output_ty: &TokenStream,
    struct_ident: &syn::Ident,
) -> syn::Result<TokenStream> {
    let Some((id, sink)) = auto_wire_names(&func.sig, args, output)? else {
        return Ok(quote! {});
    };
    let vis = &func.vis;
    let operative_ident = format_ident!("__FlezhaAutoOperative{}", struct_ident);
    auto_operative(AutoWiring {
        vis: quote! { #vis },
        ident: &operative_ident,
        id,
        input_ty: params.input_ty,
        input_name: args.input.as_ref(),
        on_full: on_full_call(&args.on_full)?,
        chain: quote! { .into::<#struct_ident>(); },
        payload: output_ty.clone(),
        sink,
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

fn resolve_output(ret: &ReturnType) -> syn::Result<Output> {
    let ty = match ret {
        ReturnType::Default => {
            return Ok(Output {
                shape: Shape::Unit,
                result: false,
            })
        }
        ReturnType::Type(_, ty) => ty,
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
    Ok(Output { shape, result })
}

fn is_unit(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(t) if t.elems.is_empty())
}

/// The referent of a `&mut _` parameter names `ProcedureCtx`. Matched on the
/// last path segment so every import spelling (`ProcedureCtx`,
/// `flezha_runner::ProcedureCtx`, a lifetime-annotated form) classifies alike.
fn is_procedure_ctx(ty: &Type) -> bool {
    matches!(ty, Type::Path(tp) if tp
        .path
        .segments
        .last()
        .is_some_and(|s| s.ident == "ProcedureCtx"))
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
        "drop_nothing" => quote! { DropNothing },
        "drop_newest" => quote! { DropNewest },
        "drop_oldest" => quote! { DropOldest },
        _ => return Err(syn::Error::new_spanned(
            id,
            "#[flezha::procedure] `on_full` must be one of: drop_nothing, drop_newest, drop_oldest",
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
            "id" => args.id = Some(try_str(&value)?),
            "sink" => args.sink = Some(try_str(&value)?),
            "input" => args.input = Some(try_lit_str(&value)?.clone()),
            "init" => args.init = Some(try_path(&value)?),
            "key" => args.key = Some(try_path(&value)?),
            "emits" => args.emits = Some(try_type(&value)?),
            "keys" => args.keys = Some(try_array(&value)?),
            "entities_for" => args.entities_for = Some(try_path(&value)?),
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
            "on_full" => args.on_full = Some(try_ident(&value)?),
            "clock" => args.clock = Some(try_type(&value)?),
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

fn try_str(expr: &Expr) -> syn::Result<String> {
    Ok(try_lit_str(expr)?.value())
}

fn try_lit_str(expr: &Expr) -> syn::Result<&LitStr> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(s), ..
        }) => Ok(s),
        _ => Err(syn::Error::new_spanned(expr, "expected a string literal")),
    }
}

fn try_array(expr: &Expr) -> syn::Result<Vec<Expr>> {
    match expr {
        Expr::Array(arr) => Ok(arr.elems.iter().cloned().collect()),
        _ => Err(syn::Error::new_spanned(
            expr,
            "expected an array literal, e.g. `keys = [\"a\", \"b\"]`",
        )),
    }
}

fn try_path(expr: &Expr) -> syn::Result<syn::Path> {
    match expr {
        Expr::Path(p) => Ok(p.path.clone()),
        _ => Err(syn::Error::new_spanned(
            expr,
            "expected a function path, e.g. `entities_for = my_fn`",
        )),
    }
}

fn try_ident(expr: &Expr) -> syn::Result<syn::Ident> {
    let ident = match expr {
        Expr::Path(p) => p.path.get_ident().cloned(),
        _ => None,
    };
    ident.ok_or_else(|| syn::Error::new_spanned(expr, "expected a bare identifier"))
}

fn try_type(expr: &Expr) -> syn::Result<Type> {
    match expr {
        Expr::Path(p) => Ok(Type::Path(syn::TypePath {
            qself: p.qself.clone(),
            path: p.path.clone(),
        })),
        _ => Err(syn::Error::new_spanned(expr, "expected a type path")),
    }
}
