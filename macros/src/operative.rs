//! `flezha::operative! { id: "...", pipeline: input::<T> -> A -> B -> plugin::<U>("k") }`
//! — lowers a strictly linear pipeline to a `channels()` body + inventory
//! auto-registration. Fan-out / multi-input is refused with a message
//! pointing at an explicit `channels()`. See `docs/sdk/authoring_contract.md`
//! §5.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{Ident, LitStr, Path, Token, Type};

use crate::util::snake_to_pascal;

struct OperativeDef {
    id: String,
    input_ty: Type,
    /// The entry channel name written after the head's turbofish, if any.
    input_name: Option<LitStr>,
    procedures: Vec<Path>,
    plugin_ty: Type,
    plugin_name: String,
}

/// The next token as the keyword `want`, or `msg` spanned on what stands
/// there instead. The identifier is returned because a later refusal spans off
/// it.
fn keyword(input: ParseStream, want: &str, msg: &str) -> syn::Result<Ident> {
    let kw: Ident = input.parse()?;
    if kw != want {
        return Err(syn::Error::new_spanned(kw, msg));
    }
    Ok(kw)
}

/// The `::<T>` a keyword carries its type in.
fn parse_turbofish_ty(input: ParseStream) -> syn::Result<Type> {
    input.parse::<Token![::]>()?;
    input.parse::<Token![<]>()?;
    let ty: Type = input.parse()?;
    input.parse::<Token![>]>()?;
    Ok(ty)
}

/// The `("name")` an end of the pipeline carries its channel name in.
fn parse_paren_name(input: ParseStream) -> syn::Result<LitStr> {
    let content;
    syn::parenthesized!(content in input);
    content.parse()
}

/// A parsed pipeline tail: the procedure hops in order, and the terminal
/// `plugin::<U>("name")` when one was written.
type Pipeline = (Vec<Path>, Option<(Type, String)>);

/// The `-> procedure -> ... -> plugin::<U>("name")` tail. Stops at the plugin
/// hop; whatever follows it is left for `parse`'s fan-out refusal to name.
fn parse_pipeline(input: ParseStream) -> syn::Result<Pipeline> {
    let mut procedures: Vec<Path> = Vec::new();
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
            let ty = parse_turbofish_ty(input)?;
            let name = parse_paren_name(input)?;
            plugin = Some((ty, name.value()));
            break;
        }
        procedures.push(input.parse::<Path>()?);
    }
    Ok((procedures, plugin))
}

impl Parse for OperativeDef {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        keyword(input, "id", "expected `id:`")?;
        input.parse::<Token![:]>()?;
        let id: LitStr = input.parse()?;
        input.parse::<Token![,]>()?;

        keyword(input, "pipeline", "expected `pipeline:`")?;
        input.parse::<Token![:]>()?;

        let input_kw = keyword(input, "input", "pipeline must start with `input::<T>`")?;
        let input_ty = parse_turbofish_ty(input)?;
        let input_name = input
            .peek(syn::token::Paren)
            .then(|| parse_paren_name(input))
            .transpose()?;

        let (procedures, plugin) = parse_pipeline(input)?;

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

        if procedures.is_empty() {
            return Err(syn::Error::new(
                input_kw.span(),
                "pipeline must contain at least one procedure between `input` and `plugin`",
            ));
        }

        Ok(OperativeDef {
            id: id.value(),
            input_ty,
            input_name,
            procedures,
            plugin_ty,
            plugin_name,
        })
    }
}

/// The hidden `StreamOperative` an auto-wire path emits: one entry channel
/// threaded through a procedure chain, the terminal payload type pinned, the
/// stream sunk under `sink`, and an `AutoOperative` submitted so a runner
/// harvests the class without a `define_stream` call.
pub(crate) struct AutoWiring<'a> {
    /// Visibility of the generated struct — the procedure's own on the
    /// attribute path, private for `operative!`.
    pub vis: TokenStream,
    pub ident: &'a Ident,
    pub id: &'a str,
    pub input_ty: &'a Type,
    /// The entry channel's name as the author wrote it; `None` takes the
    /// default, which is what every operative that names nothing ships under.
    pub input_name: Option<&'a LitStr>,
    /// The entry channel's `.on_full(...)`, empty when none was declared.
    pub on_full: TokenStream,
    /// The `.into::<P>()` hops the entry stream is threaded through, including
    /// the statement's own `;` and any further `let stream = ...` hops.
    pub chain: TokenStream,
    /// The type the terminal stream's payload is pinned against.
    pub payload: TokenStream,
    /// The plugin sink's name; the producing channel is named after it.
    pub sink: &'a str,
}

/// The entry channel's name: the author's where one was written, and `input`
/// otherwise. An empty one is refused — a channel nothing can address.
fn entry_channel_name(written: Option<&LitStr>) -> syn::Result<String> {
    match written {
        None => Ok("input".to_string()),
        Some(lit) if lit.value().is_empty() => Err(syn::Error::new_spanned(
            lit,
            "an entry channel name may not be empty",
        )),
        Some(lit) => Ok(lit.value()),
    }
}

pub(crate) fn auto_operative(wiring: AutoWiring<'_>) -> syn::Result<TokenStream> {
    let AutoWiring {
        vis,
        ident,
        id,
        input_ty,
        input_name,
        on_full,
        chain,
        payload,
        sink,
    } = wiring;
    let entry = entry_channel_name(input_name)?;
    Ok(quote! {
        #[doc(hidden)]
        #vis struct #ident;
        impl ::flezha_runner::StreamOperative for #ident {
            fn factory_id() -> &'static str { #id }
            fn channels(c: &::flezha_runner::ChannelBuilder) {
                let stream = c.input::<#input_ty>(#entry) #on_full #chain
                // Pins the declared sink payload type against the terminal
                // procedure's `Output`.
                let stream: ::flezha_runner::Stream<'_, #payload> = stream;
                stream.sink_named(#sink);
            }
        }
        ::flezha_runner::inventory::submit! {
            ::flezha_runner::AutoOperative::new(
                <#ident as ::flezha_runner::StreamOperative>::factory_id,
                || ::flezha_runner::__register_stream::<#ident>(),
            )
        }
    })
}

pub fn expand(item: TokenStream) -> syn::Result<TokenStream> {
    let def: OperativeDef = syn::parse2(item)?;

    let first_procedure = &def.procedures[0];
    let plugin_ty = &def.plugin_ty;
    let ident = format_ident!("__FlezhaOperative_{}", sanitize_ident(&def.id));

    // Chained hops on the stream handle; each `.into` returns the consumed
    // procedure's output stream.
    let hops: TokenStream = def
        .procedures
        .iter()
        .skip(1)
        .map(|cons| {
            quote! {
                let stream = stream.into::<#cons>();
            }
        })
        .collect();

    auto_operative(AutoWiring {
        vis: quote! {},
        ident: &ident,
        id: &def.id,
        input_ty: &def.input_ty,
        input_name: def.input_name.as_ref(),
        on_full: quote! {},
        chain: quote! { .into::<#first_procedure>(); #hops },
        payload: quote! { #plugin_ty },
        sink: &def.plugin_name,
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
