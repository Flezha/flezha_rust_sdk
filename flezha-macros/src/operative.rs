//! `flezha::operative! { id: "...", pipeline: input::<T> -> A -> B -> plugin::<U>("k") }`
//! — lowers a strictly linear pipeline to a `channels()` body + inventory
//! auto-registration. Fan-out / multi-input is rejected with a message
//! pointing at an explicit `channels()`. See `docs/sdk_authoring_contract.md`
//! §5 and design D6.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{Ident, LitStr, Path, Token, Type};

use crate::util::snake_to_pascal;

struct OperativeDef {
    id: String,
    input_ty: Type,
    operations: Vec<Path>,
    plugin_ty: Type,
    plugin_name: String,
}

impl Parse for OperativeDef {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        // `id: "..."`,
        let id_kw: Ident = input.parse()?;
        if id_kw != "id" {
            return Err(syn::Error::new_spanned(id_kw, "expected `id:`"));
        }
        input.parse::<Token![:]>()?;
        let id: LitStr = input.parse()?;
        input.parse::<Token![,]>()?;

        // `pipeline:`
        let pipe_kw: Ident = input.parse()?;
        if pipe_kw != "pipeline" {
            return Err(syn::Error::new_spanned(pipe_kw, "expected `pipeline:`"));
        }
        input.parse::<Token![:]>()?;

        // `input::<T>`
        let input_kw: Ident = input.parse()?;
        if input_kw != "input" {
            return Err(syn::Error::new_spanned(
                input_kw,
                "pipeline must start with `input::<T>`",
            ));
        }
        input.parse::<Token![::]>()?;
        input.parse::<Token![<]>()?;
        let input_ty: Type = input.parse()?;
        input.parse::<Token![>]>()?;

        // `-> operation -> operation -> ... -> plugin::<U>("name")`
        let mut operations: Vec<Path> = Vec::new();
        let mut plugin: Option<(Type, String)> = None;
        while input.peek(Token![->]) {
            input.parse::<Token![->]>()?;
            let ahead = input.fork();
            let is_plugin = ahead
                .parse::<Ident>()
                .map(|i| i == "plugin")
                .unwrap_or(false);
            if is_plugin {
                input.parse::<Ident>()?; // plugin
                input.parse::<Token![::]>()?;
                input.parse::<Token![<]>()?;
                let ty: Type = input.parse()?;
                input.parse::<Token![>]>()?;
                let content;
                syn::parenthesized!(content in input);
                let name: LitStr = content.parse()?;
                plugin = Some((ty, name.value()));
                break;
            }
            operations.push(input.parse::<Path>()?);
        }

        if !input.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "fan-out / multi-input pipelines are not supported by `flezha::operative!`; \
                 declare the topology with an explicit `channels()` body",
            ));
        }

        let (plugin_ty, plugin_name) = plugin.ok_or_else(|| {
            syn::Error::new(
                input_kw.span(),
                "pipeline must end with `-> plugin::<U>(\"name\")`",
            )
        })?;

        if operations.is_empty() {
            return Err(syn::Error::new(
                input_kw.span(),
                "pipeline must contain at least one operation between `input` and `plugin`",
            ));
        }

        Ok(OperativeDef {
            id: id.value(),
            input_ty,
            operations,
            plugin_ty,
            plugin_name,
        })
    }
}

pub fn expand(item: TokenStream) -> syn::Result<TokenStream> {
    let def: OperativeDef = syn::parse2(item)?;

    let id = &def.id;
    let input_ty = &def.input_ty;
    let plugin_ty = &def.plugin_ty;
    let plugin_name = &def.plugin_name;
    let first_operation = &def.operations[0];
    let last_operation = def.operations.last().unwrap();

    let operative_ident = format_ident!("__FlezhaOperative_{}", sanitize_ident(&def.id));

    // Inter-operation hops carry the producer's `Output` type.
    let hops = def.operations.windows(2).enumerate().map(|(i, pair)| {
        let prod = &pair[0];
        let cons = &pair[1];
        let hop_name = format!("__hop{}", i + 1);
        quote! {
            c.channel::<<#prod as ::flezha_runner::Operation>::Output>(#hop_name)
                .from::<#prod>()
                .into::<#cons>();
        }
    });

    Ok(quote! {
        #[doc(hidden)]
        struct #operative_ident;
        impl ::flezha_runner::UnboundedOperative for #operative_ident {
            fn factory_id() -> &'static str { #id }
            fn channels(c: &mut ::flezha_runner::ChannelBuilder) {
                c.input::<#input_ty>("input").into::<#first_operation>();
                #(#hops)*
                c.channel::<#plugin_ty>(#plugin_name)
                    .from::<#last_operation>()
                    .to_plugin(#plugin_name);
            }
        }
        ::flezha_runner::inventory::submit! {
            ::flezha_runner::AutoOperative::new(
                <#operative_ident as ::flezha_runner::UnboundedOperative>::factory_id,
                || ::flezha_runner::__register_unbounded::<#operative_ident>(),
            )
        }
    })
}

/// Make a struct-name suffix from a factory id string.
fn sanitize_ident(id: &str) -> String {
    let cleaned: String = id
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();
    snake_to_pascal(&cleaned)
}
