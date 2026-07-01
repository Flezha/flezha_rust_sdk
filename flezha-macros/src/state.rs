//! `#[derive(flezha::State)]` — generates a field-granular `Change` enum,
//! `apply_change`, the `DerivedState` binding, and journaling mutators on the
//! state handle. See `docs/sdk_authoring_contract.md` §3.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Field, Fields};

use crate::util::{first_generic_arg, map_kv, snake_ident_to_pascal};

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

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
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

    let change_ident = format_ident!("{}Change", state_ident);
    let mut_trait_ident = format_ident!("{}Mut", state_ident);

    let mut variants = Vec::new();
    let mut apply_arms = Vec::new();
    let mut mutator_sigs = Vec::new();
    let mut mutator_impls = Vec::new();

    for field in fields {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_ty = &field.ty;
        let pascal = snake_ident_to_pascal(field_ident);

        match journal_mode(field)? {
            Some(mode) => expand_map_field(
                &change_ident,
                field_ident,
                field_ty,
                &pascal,
                mode,
                &mut variants,
                &mut apply_arms,
                &mut mutator_sigs,
                &mut mutator_impls,
            )?,
            None => {
                // A `HashMap<..>` field without `#[journal(...)]` is an
                // unsupported mutation: there is no single per-entry op to
                // record. Direct the author to a hand-written `Change`.
                if map_kv(field_ty).is_some() {
                    return Err(syn::Error::new_spanned(
                        field,
                        "#[derive(flezha::State)]: a map field needs a \
                         `#[journal(map_set | map_vec | map_deque)]` annotation to derive its \
                         per-key journaling mutators; hand-write a `Change` for anything richer",
                    ));
                }

                // Every flat field gets a `Set<Field>(T)` variant + `set_<field>`.
                let set_variant = format_ident!("Set{}", pascal);
                variants.push(quote! { #set_variant(#field_ty) });
                apply_arms.push(quote! {
                    #change_ident::#set_variant(__v) => { state.#field_ident = __v.clone(); }
                });
                let set_method = format_ident!("set_{}", field_ident);
                mutator_sigs.push(quote! { fn #set_method(&mut self, value: #field_ty); });
                mutator_impls.push(quote! {
                    fn #set_method(&mut self, value: #field_ty) {
                        self.apply(#change_ident::#set_variant(value));
                    }
                });

                // `Vec<U>` fields additionally get `Push<Field>(U)` + `push_<field>`.
                if let Some(elem_ty) = first_generic_arg(field_ty, "Vec") {
                    let push_variant = format_ident!("Push{}", pascal);
                    variants.push(quote! { #push_variant(#elem_ty) });
                    apply_arms.push(quote! {
                        #change_ident::#push_variant(__v) => { state.#field_ident.push(__v.clone()); }
                    });
                    let push_method = format_ident!("push_{}", field_ident);
                    mutator_sigs.push(quote! { fn #push_method(&mut self, value: #elem_ty); });
                    mutator_impls.push(quote! {
                        fn #push_method(&mut self, value: #elem_ty) {
                            self.apply(#change_ident::#push_variant(value));
                        }
                    });
                }
            }
        }
    }

    let change_doc = format!(
        "Field-granular change for `{state}`, generated by `#[derive(flezha::State)]`.",
        state = state_ident
    );
    let trait_doc = format!(
        "Journaling mutators for `{state}` on a `StateHandle`, generated by \
         `#[derive(flezha::State)]`.",
        state = state_ident
    );

    Ok(quote! {
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

        impl ::flezha_executor::Change for #change_ident {}

        impl ::flezha_executor::DerivedState for #state_ident {
            type Change = #change_ident;
            fn apply_change(state: &mut Self, change: &Self::Change) {
                match change {
                    #(#apply_arms),*
                }
            }
        }

        #[doc = #trait_doc]
        pub trait #mut_trait_ident {
            #(#mutator_sigs)*
        }

        impl<'__sh, __S> #mut_trait_ident for ::flezha_executor::StateHandle<'__sh, __S>
        where
            __S: ::flezha_executor::Operation<State = #state_ident, Change = #change_ident>,
        {
            #(#mutator_impls)*
        }
    })
}

/// Expand a `#[journal(...)]` map field into per-key mutators, `Change`
/// variants, and `apply_change` arms. The mutators journal an explicit per-key
/// delta; the generated `apply_change` replays it (insert-default-then-op for
/// the collection modes), so the live and replay paths share one definition.
#[allow(clippy::too_many_arguments)]
fn expand_map_field(
    change_ident: &syn::Ident,
    field_ident: &syn::Ident,
    field_ty: &syn::Type,
    pascal: &syn::Ident,
    mode: JournalMode,
    variants: &mut Vec<TokenStream>,
    apply_arms: &mut Vec<TokenStream>,
    mutator_sigs: &mut Vec<TokenStream>,
    mutator_impls: &mut Vec<TokenStream>,
) -> syn::Result<()> {
    let (key_ty, value_ty) = map_kv(field_ty).ok_or_else(|| {
        syn::Error::new_spanned(
            field_ty,
            "#[journal(...)] requires a `HashMap<K, V>` field; hand-write a `Change` for \
             anything richer",
        )
    })?;

    let mut push_mutator =
        |variant: syn::Ident, method: syn::Ident, arg_ty: &syn::Type, apply_body: TokenStream| {
            variants.push(quote! { #variant(#key_ty, #arg_ty) });
            apply_arms.push(quote! {
                #change_ident::#variant(__k, __v) => { #apply_body }
            });
            mutator_sigs.push(quote! { fn #method(&mut self, key: #key_ty, value: #arg_ty); });
            mutator_impls.push(quote! {
                fn #method(&mut self, key: #key_ty, value: #arg_ty) {
                    self.apply(#change_ident::#variant(key, value));
                }
            });
        };

    match mode {
        JournalMode::MapSet => {
            // map_set: `set_<field>(key, V)` / `Set<Field>(K, V)`.
            push_mutator(
                format_ident!("Set{}", pascal),
                format_ident!("set_{}", field_ident),
                value_ty,
                quote! { state.#field_ident.insert(__k.clone(), __v.clone()); },
            );
        }
        JournalMode::MapVec => {
            let elem_ty = first_generic_arg(value_ty, "Vec").ok_or_else(|| {
                syn::Error::new_spanned(
                    field_ty,
                    "#[journal(map_vec)] requires a `HashMap<K, Vec<U>>` field; hand-write a \
                     `Change` for anything richer",
                )
            })?;
            push_mutator(
                format_ident!("Push{}", pascal),
                format_ident!("push_{}", field_ident),
                elem_ty,
                quote! { state.#field_ident.entry(__k.clone()).or_default().push(__v.clone()); },
            );
            push_mutator(
                format_ident!("ReplaceLast{}", pascal),
                format_ident!("replace_last_{}", field_ident),
                elem_ty,
                quote! {
                    let __e = state.#field_ident.entry(__k.clone()).or_default();
                    match __e.last_mut() {
                        ::core::option::Option::Some(__last) => { *__last = __v.clone(); }
                        ::core::option::Option::None => { __e.push(__v.clone()); }
                    }
                },
            );
            // drain_<field>_front(key, usize): drop the first `n` elements.
            let drain_variant = format_ident!("Drain{}Front", pascal);
            let drain_method = format_ident!("drain_{}_front", field_ident);
            variants.push(quote! { #drain_variant(#key_ty, usize) });
            apply_arms.push(quote! {
                #change_ident::#drain_variant(__k, __n) => {
                    let __e = state.#field_ident.entry(__k.clone()).or_default();
                    let __n = (*__n).min(__e.len());
                    __e.drain(0..__n);
                }
            });
            mutator_sigs.push(quote! { fn #drain_method(&mut self, key: #key_ty, n: usize); });
            mutator_impls.push(quote! {
                fn #drain_method(&mut self, key: #key_ty, n: usize) {
                    self.apply(#change_ident::#drain_variant(key, n));
                }
            });
        }
        JournalMode::MapDeque => {
            let elem_ty = first_generic_arg(value_ty, "VecDeque").ok_or_else(|| {
                syn::Error::new_spanned(
                    field_ty,
                    "#[journal(map_deque)] requires a `HashMap<K, VecDeque<U>>` field; \
                     hand-write a `Change` for anything richer",
                )
            })?;
            push_mutator(
                format_ident!("PushBack{}", pascal),
                format_ident!("push_back_{}", field_ident),
                elem_ty,
                quote! {
                    state.#field_ident.entry(__k.clone()).or_default().push_back(__v.clone());
                },
            );
            // pop_front_<field>(key, usize): drop the first `n` elements.
            let pop_variant = format_ident!("PopFront{}", pascal);
            let pop_method = format_ident!("pop_front_{}", field_ident);
            variants.push(quote! { #pop_variant(#key_ty, usize) });
            apply_arms.push(quote! {
                #change_ident::#pop_variant(__k, __n) => {
                    let __e = state.#field_ident.entry(__k.clone()).or_default();
                    for _ in 0..(*__n).min(__e.len()) { __e.pop_front(); }
                }
            });
            mutator_sigs.push(quote! { fn #pop_method(&mut self, key: #key_ty, n: usize); });
            mutator_impls.push(quote! {
                fn #pop_method(&mut self, key: #key_ty, n: usize) {
                    self.apply(#change_ident::#pop_variant(key, n));
                }
            });
        }
    }

    Ok(())
}
