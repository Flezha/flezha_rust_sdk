//! `#[derive(flezha::State)]` — generates a field-granular `Change` enum,
//! `apply_change`, the `DerivedState` binding, and journaling mutators on the
//! state handle. See `docs/sdk/authoring_contract.md` §3.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{Data, DeriveInput, Field, Fields, Ident};

use crate::util::{generic_args, snake_ident_to_pascal};

/// Per-key journaling mode declared by `#[journal(...)]` on a map field.
#[derive(Clone, Copy)]
#[allow(clippy::enum_variant_names)] // names mirror the `map_*` annotation values
enum JournalMode {
    MapSet,
    MapVec,
    MapDeque,
}

/// Read the `#[journal(map_set | map_vec | map_deque)]` annotation off a field.
fn journal_mode(field: &Field) -> syn::Result<Option<JournalMode>> {
    for attr in &field.attrs {
        if !attr.path().is_ident("journal") {
            continue;
        }
        let ident: syn::Ident = attr.parse_args()?;
        let mode = match ident.to_string().as_str() {
            "map_set" => JournalMode::MapSet,
            "map_vec" => JournalMode::MapVec,
            "map_deque" => JournalMode::MapDeque,
            other => {
                return Err(syn::Error::new_spanned(
                    ident,
                    format!(
                        "#[journal]: unknown mode `{other}`; expected `map_set`, `map_vec`, \
                         or `map_deque`"
                    ),
                ))
            }
        };
        return Ok(Some(mode));
    }
    Ok(None)
}

/// What a mutation's second argument is: the value it journals, or the count a
/// window-trimming op takes.
#[derive(Clone, Copy)]
enum Arg {
    Value,
    Count,
}

/// The four token streams every derived mutation contributes — one `Change`
/// variant, one `apply_change` arm, the mutator's signature and its body — kept
/// together so a site cannot add three of them and forget the fourth.
struct Mutators {
    change_ident: syn::Ident,
    variants: Vec<TokenStream>,
    apply_arms: Vec<TokenStream>,
    sigs: Vec<TokenStream>,
    impls: Vec<TokenStream>,
}

impl Mutators {
    fn new(change_ident: syn::Ident) -> Self {
        Self {
            change_ident,
            variants: Vec::new(),
            apply_arms: Vec::new(),
            sigs: Vec::new(),
            impls: Vec::new(),
        }
    }

    /// One journaled mutation. `key_ty` is `Some` for a per-key map field,
    /// whose variant and mutator both carry the key ahead of the argument;
    /// `apply_body` replays the delta with the key bound as `__k` and the
    /// argument as `__v` (a value) or `__n` (a count).
    fn push(
        &mut self,
        variant: syn::Ident,
        method: syn::Ident,
        key_ty: Option<&syn::Type>,
        arg: (Arg, TokenStream),
        apply_body: TokenStream,
    ) {
        let change_ident = &self.change_ident;
        let (arg_kind, arg_ty) = arg;
        let (param, bind) = match arg_kind {
            Arg::Value => (quote! { value }, quote! { __v }),
            Arg::Count => (quote! { n }, quote! { __n }),
        };
        let (variant_key, sig_key, call_key, bind_key) = match key_ty {
            Some(k) => (
                quote! { #k, },
                quote! { key: #k, },
                quote! { key, },
                quote! { __k, },
            ),
            None => (quote! {}, quote! {}, quote! {}, quote! {}),
        };
        self.variants
            .push(quote! { #variant(#variant_key #arg_ty) });
        self.apply_arms.push(quote! {
            #change_ident::#variant(#bind_key #bind) => { #apply_body }
        });
        self.sigs
            .push(quote! { fn #method(&mut self, #sig_key #param: #arg_ty); });
        self.impls.push(quote! {
            fn #method(&mut self, #sig_key #param: #arg_ty) {
                self.apply(#change_ident::#variant(#call_key #param));
            }
        });
    }
}

/// The named fields a `Change` can be derived from. Anything else — a tuple
/// struct, an enum, a struct with no fields — is refused here.
fn resolve_named_fields(input: &DeriveInput) -> syn::Result<&Punctuated<Field, Comma>> {
    let state_ident = &input.ident;
    let fields = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    state_ident,
                    "#[derive(flezha::State)] requires a struct with named fields; \
                     hand-write a `Change` for anything richer",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                state_ident,
                "#[derive(flezha::State)] can only be applied to a struct; a mutation that is \
                 conditional, multi-field-atomic, or computed needs a hand-written `Change`",
            ))
        }
    };

    if fields.is_empty() {
        return Err(syn::Error::new_spanned(
            state_ident,
            "#[derive(flezha::State)] requires at least one field",
        ));
    }
    Ok(fields)
}

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let state_ident = &input.ident;
    let fields = resolve_named_fields(&input)?;

    let change_ident = format_ident!("{}Change", state_ident);
    let mut out = Mutators::new(change_ident.clone());

    for field in fields {
        let field_ident = field.ident.as_ref().expect("named field");
        let pascal = snake_ident_to_pascal(field_ident);
        match journal_mode(field)? {
            Some(mode) => expand_map_field(field_ident, &field.ty, &pascal, mode, &mut out)?,
            None => expand_flat_field(field, &pascal, &mut out)?,
        }
    }

    Ok(expand_items(state_ident, &change_ident, &out))
}

/// The four generated items: the `Change` enum, its marker impl, the
/// `DerivedState` fold, and the mutator trait with its `StateHandle` impl.
fn expand_items(state_ident: &Ident, change_ident: &Ident, out: &Mutators) -> TokenStream {
    let mut_trait_ident = format_ident!("{}Mut", state_ident);
    let change_doc = format!(
        "Field-granular change for `{state_ident}`, generated by `#[derive(flezha::State)]`."
    );
    let trait_doc = format!(
        "Journaling mutators for `{state_ident}` on a `StateHandle`, generated by \
         `#[derive(flezha::State)]`."
    );
    let (variants, apply_arms) = (&out.variants, &out.apply_arms);
    let (sigs, impls) = (&out.sigs, &out.impls);

    quote! {
        #[doc = #change_doc]
        #[derive(
            ::core::clone::Clone,
            ::core::fmt::Debug,
            ::core::cmp::PartialEq,
            ::serde::Serialize,
            ::serde::Deserialize,
        )]
        pub enum #change_ident {
            #(#variants),*
        }

        impl ::flezha_runner::Change for #change_ident {}

        impl ::flezha_runner::DerivedState for #state_ident {
            type Change = #change_ident;
            fn apply_change(state: &mut Self, change: &Self::Change) {
                match change {
                    #(#apply_arms),*
                }
            }
        }

        #[doc = #trait_doc]
        pub trait #mut_trait_ident {
            #(#sigs)*
        }

        impl<'__sh, __S> #mut_trait_ident for ::flezha_runner::StateHandle<'__sh, __S>
        where
            __S: ::flezha_runner::Procedure<State = #state_ident, Change = #change_ident>,
        {
            #(#impls)*
        }
    }
}

/// Expand a field carrying no `#[journal(...)]`: a `Set<Field>` mutator for
/// every field, `Push<Field>` for a `Vec`, and the sliding-window
/// `PushBack<Field>` / `PopFront<Field>` pair for a `VecDeque`.
fn expand_flat_field(field: &Field, pascal: &syn::Ident, out: &mut Mutators) -> syn::Result<()> {
    let field_ident = field.ident.as_ref().expect("named field");
    let field_ty = &field.ty;

    // A `HashMap<..>` field without `#[journal(...)]` is an unsupported
    // mutation: there is no single per-entry op to record. Direct the author
    // to a hand-written `Change`.
    if generic_args(field_ty, "HashMap").nth(1).is_some() {
        return Err(syn::Error::new_spanned(
            field,
            "#[derive(flezha::State)]: a map field needs a \
             `#[journal(map_set | map_vec | map_deque)]` annotation to derive its \
             per-key journaling mutators; hand-write a `Change` for anything richer",
        ));
    }

    out.push(
        format_ident!("Set{}", pascal),
        format_ident!("set_{}", field_ident),
        None,
        (Arg::Value, quote! { #field_ty }),
        quote! { state.#field_ident = __v.clone(); },
    );

    expand_seq_mutators(field_ident, field_ty, pascal, out);
    Ok(())
}

/// The whole-collection mutators a bare `Vec` or `VecDeque` field carries. A
/// `VecDeque<U>` takes the same deltas `map_deque` records, for a window whose
/// entity key already is what the map would have keyed by; without them a
/// per-entity window could only journal whole-collection sets.
fn expand_seq_mutators(
    field_ident: &syn::Ident,
    field_ty: &syn::Type,
    pascal: &syn::Ident,
    out: &mut Mutators,
) {
    if let Some(elem_ty) = generic_args(field_ty, "Vec").next() {
        out.push(
            format_ident!("Push{}", pascal),
            format_ident!("push_{}", field_ident),
            None,
            (Arg::Value, quote! { #elem_ty }),
            quote! { state.#field_ident.push(__v.clone()); },
        );
    }

    if let Some(elem_ty) = generic_args(field_ty, "VecDeque").next() {
        out.push(
            format_ident!("PushBack{}", pascal),
            format_ident!("push_back_{}", field_ident),
            None,
            (Arg::Value, quote! { #elem_ty }),
            quote! { state.#field_ident.push_back(__v.clone()); },
        );
        out.push(
            format_ident!("PopFront{}", pascal),
            format_ident!("pop_front_{}", field_ident),
            None,
            (Arg::Count, quote! { usize }),
            quote! {
                for _ in 0..(*__n).min(state.#field_ident.len()) {
                    state.#field_ident.pop_front();
                }
            },
        );
    }
}

/// Expand a `#[journal(...)]` map field into per-key mutators, `Change`
/// variants, and `apply_change` arms. The mutators journal an explicit per-key
/// delta; the generated `apply_change` replays it (insert-default-then-op for
/// the collection modes), so the live and replay paths share one definition.
fn expand_map_field(
    field_ident: &syn::Ident,
    field_ty: &syn::Type,
    pascal: &syn::Ident,
    mode: JournalMode,
    out: &mut Mutators,
) -> syn::Result<()> {
    let mut map_args = generic_args(field_ty, "HashMap");
    let (Some(key_ty), Some(value_ty)) = (map_args.next(), map_args.next()) else {
        return Err(syn::Error::new_spanned(
            field_ty,
            "#[journal(...)] requires a `HashMap<K, V>` field; hand-write a `Change` for \
             anything richer",
        ));
    };

    match mode {
        JournalMode::MapSet => expand_map_set(field_ident, pascal, key_ty, value_ty, out),
        JournalMode::MapVec => expand_map_vec(field_ident, field_ty, pascal, key_ty, value_ty, out),
        JournalMode::MapDeque => {
            expand_map_deque(field_ident, field_ty, pascal, key_ty, value_ty, out)
        }
    }
}

/// `map_set`: `set_<field>(key, V)` / `Set<Field>(K, V)`.
fn expand_map_set(
    field_ident: &syn::Ident,
    pascal: &syn::Ident,
    key_ty: &syn::Type,
    value_ty: &syn::Type,
    out: &mut Mutators,
) -> syn::Result<()> {
    out.push(
        format_ident!("Set{}", pascal),
        format_ident!("set_{}", field_ident),
        Some(key_ty),
        (Arg::Value, quote! { #value_ty }),
        quote! { state.#field_ident.insert(__k.clone(), __v.clone()); },
    );
    Ok(())
}

/// `map_vec`: append, replace-the-last, and drop-the-first-`n` per key.
fn expand_map_vec(
    field_ident: &syn::Ident,
    field_ty: &syn::Type,
    pascal: &syn::Ident,
    key_ty: &syn::Type,
    value_ty: &syn::Type,
    out: &mut Mutators,
) -> syn::Result<()> {
    let Some(elem_ty) = generic_args(value_ty, "Vec").next() else {
        return Err(syn::Error::new_spanned(
            field_ty,
            "#[journal(map_vec)] requires a `HashMap<K, Vec<U>>` field; hand-write a \
             `Change` for anything richer",
        ));
    };
    out.push(
        format_ident!("Push{}", pascal),
        format_ident!("push_{}", field_ident),
        Some(key_ty),
        (Arg::Value, quote! { #elem_ty }),
        quote! { state.#field_ident.entry(__k.clone()).or_default().push(__v.clone()); },
    );
    out.push(
        format_ident!("ReplaceLast{}", pascal),
        format_ident!("replace_last_{}", field_ident),
        Some(key_ty),
        (Arg::Value, quote! { #elem_ty }),
        quote! {
            let __e = state.#field_ident.entry(__k.clone()).or_default();
            match __e.last_mut() {
                ::core::option::Option::Some(__last) => { *__last = __v.clone(); }
                ::core::option::Option::None => { __e.push(__v.clone()); }
            }
        },
    );
    out.push(
        format_ident!("Drain{}Front", pascal),
        format_ident!("drain_{}_front", field_ident),
        Some(key_ty),
        (Arg::Count, quote! { usize }),
        quote! {
            let __e = state.#field_ident.entry(__k.clone()).or_default();
            let __n = (*__n).min(__e.len());
            __e.drain(0..__n);
        },
    );
    Ok(())
}

/// `map_deque`: push-back and pop-the-first-`n` per key.
fn expand_map_deque(
    field_ident: &syn::Ident,
    field_ty: &syn::Type,
    pascal: &syn::Ident,
    key_ty: &syn::Type,
    value_ty: &syn::Type,
    out: &mut Mutators,
) -> syn::Result<()> {
    let Some(elem_ty) = generic_args(value_ty, "VecDeque").next() else {
        return Err(syn::Error::new_spanned(
            field_ty,
            "#[journal(map_deque)] requires a `HashMap<K, VecDeque<U>>` field; \
             hand-write a `Change` for anything richer",
        ));
    };
    out.push(
        format_ident!("PushBack{}", pascal),
        format_ident!("push_back_{}", field_ident),
        Some(key_ty),
        (Arg::Value, quote! { #elem_ty }),
        quote! {
            state.#field_ident.entry(__k.clone()).or_default().push_back(__v.clone());
        },
    );
    out.push(
        format_ident!("PopFront{}", pascal),
        format_ident!("pop_front_{}", field_ident),
        Some(key_ty),
        (Arg::Count, quote! { usize }),
        quote! {
            let __e = state.#field_ident.entry(__k.clone()).or_default();
            for _ in 0..(*__n).min(__e.len()) { __e.pop_front(); }
        },
    );
    Ok(())
}
