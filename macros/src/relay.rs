//! `flezha::pipeline! { pub struct Name; id: "...", pipeline: input::<T> -> A -> B -> plugin::<U>("k") }`
//! — declares the named type and lowers a strictly linear pipeline to its
//! `channels()` body. The author registers it with
//! `runner.register_factory(Name)`; nothing is collected. Fan-out / multi-input
//! is refused with a message pointing at an explicit `channels()`. See
//! `docs/internal/sdk/authoring_contract.md` §5.

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, LitStr, Path, Token, Type, Visibility};

struct RelayDef {
    vis: Visibility,
    ident: Ident,
    id: String,
    input_ty: Type,
    /// The entry channel name written after the head's turbofish, if any.
    input_name: Option<LitStr>,
    tasklets: Vec<Path>,
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

/// A parsed pipeline tail: the tasklet hops in order, and the terminal
/// `plugin::<U>("name")` when one was written.
type Pipeline = (Vec<Path>, Option<(Type, String)>);

/// The `-> tasklet -> ... -> plugin::<U>("name")` tail. Stops at the plugin
/// hop; whatever follows it is left for `parse`'s fan-out refusal to name.
fn parse_pipeline(input: ParseStream) -> syn::Result<Pipeline> {
    let mut tasklets: Vec<Path> = Vec::new();
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
        tasklets.push(input.parse::<Path>()?);
    }
    Ok((tasklets, plugin))
}

impl Parse for RelayDef {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        // The macro begins with the type it declares, so the relay is a
        // nameable value the author hands to `register_factory`.
        let vis: Visibility = input.parse()?;
        input.parse::<Token![struct]>().map_err(|_| {
            syn::Error::new(
                input.span(),
                "`flezha::pipeline!` begins with the type it declares: `pub struct Name;`",
            )
        })?;
        let ident: Ident = input.parse()?;
        input.parse::<Token![;]>()?;

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

        let (tasklets, plugin) = parse_pipeline(input)?;

        if !input.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "fan-out / multi-input pipelines are not supported by `flezha::pipeline!`; \
                 declare the topology with an explicit `channels()` body",
            ));
        }

        let (plugin_ty, plugin_name) = plugin.ok_or_else(|| {
            syn::Error::new(
                input_kw.span(),
                "pipeline must end with `-> plugin::<U>(\"name\")`",
            )
        })?;

        if tasklets.is_empty() {
            return Err(syn::Error::new(
                input_kw.span(),
                "pipeline must contain at least one tasklet between `input` and `plugin`",
            ));
        }

        Ok(RelayDef {
            vis,
            ident,
            id: id.value(),
            input_ty,
            input_name,
            tasklets,
            plugin_ty,
            plugin_name,
        })
    }
}

/// A generated relay: one entry channel threaded through a tasklet
/// chain, and, when an `output` is written, the terminal payload type pinned and
/// the stream sunk under it. The relay is a type the author can name, so
/// registration is a call they write.
pub(crate) struct Wiring<'a> {
    /// The relay type's visibility; `None` when the author declared the
    /// type and the wiring only implements the trait on it.
    pub vis: Option<&'a Visibility>,
    /// The relay type, the one the author registers.
    pub ident: &'a Ident,
    pub id: &'a str,
    pub input_ty: &'a Type,
    /// The entry channel's name as the author wrote it; `None` takes the
    /// default, which is what every relay that names nothing ships under.
    pub input_name: Option<&'a LitStr>,
    /// The entry channel's `.on_full(...)`, empty when none was declared.
    pub on_full: TokenStream,
    /// The `.into::<P>()` hops the entry stream is threaded through, including
    /// the statement's own `;` and any further `let stream = ...` hops.
    pub chain: TokenStream,
    /// The type the terminal stream's payload is pinned against.
    pub payload: TokenStream,
    /// The plugin output's name, the producing channel named after it; `None`
    /// declares no plugin channel.
    pub output: Option<&'a str>,
    /// The relay items the short form sets beyond the graph (a steps relay's
    /// `each`), empty when every item keeps its trait default.
    pub settings: TokenStream,
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

/// The relay type and its impl of the `Relay` trait.
pub(crate) fn relay_item(wiring: Wiring<'_>) -> syn::Result<TokenStream> {
    let Wiring {
        vis,
        ident,
        id,
        input_ty,
        input_name,
        on_full,
        chain,
        payload,
        output,
        settings,
    } = wiring;
    let entry = entry_channel_name(input_name)?;
    let declared = vis.map(|vis| quote! { #vis struct #ident; });
    // Pins the declared output payload type against the terminal tasklet's
    // `Output`; with no output the tasklet's output is left unconsumed, as a
    // hand-written channel ending at a tasklet leaves it.
    let sunk = match output {
        Some(output) => quote! {
            let stream: ::flezha_runner::Stream<'_, #payload> = stream;
            stream.output_named(#output);
        },
        None => quote! { let _ = stream; },
    };
    Ok(quote! {
        #declared
        impl ::flezha_runner::Relay for #ident {
            fn factory_id() -> &'static str { #id }
            fn channels(c: &::flezha_runner::ChannelBuilder) {
                let stream = c.input::<#input_ty>(#entry) #on_full #chain
                #sunk
            }
            #settings
        }
    })
}

pub fn expand(item: TokenStream) -> syn::Result<TokenStream> {
    let def: RelayDef = syn::parse2(item)?;

    let first_tasklet = &def.tasklets[0];
    let plugin_ty = &def.plugin_ty;

    // Chained hops on the stream handle; each `.into` returns the consumed
    // tasklet's output stream.
    let hops: TokenStream = def
        .tasklets
        .iter()
        .skip(1)
        .map(|cons| {
            quote! {
                let stream = stream.into::<#cons>();
            }
        })
        .collect();

    relay_item(Wiring {
        vis: Some(&def.vis),
        ident: &def.ident,
        id: &def.id,
        input_ty: &def.input_ty,
        input_name: def.input_name.as_ref(),
        on_full: quote! {},
        chain: quote! { .into::<#first_tasklet>(); #hops },
        payload: quote! { #plugin_ty },
        output: Some(&def.plugin_name),
        settings: quote! {},
    })
}
