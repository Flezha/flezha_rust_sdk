//! `#[flezha::tasklet(...)]` on an `impl Tasklet for T` block. It generates no
//! type: it only completes the block with the items the author left out, the
//! `NAME`, the settings given as keys, the `*_OVERRIDDEN` flags from the hooks
//! the block writes, the `CLOCK`, a default `init_state`, and the default
//! associated types. The short-form attributes reuse the same completion. See
//! `docs/sdk/authoring_contract.md` §2.

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Expr, ExprLit, ImplItem, Item, ItemImpl, Lit, LitStr, Meta, Token};

/// The attributes that complete an `impl Tasklet` block. Two on one block
/// would each claim the tasklet's name.
const COMPLETING_ATTRIBUTES: [&str; 2] = ["tasklet", "relay"];

/// A setting key: the trait item it writes, and how it writes it.
struct Setting {
    key: &'static str,
    item: &'static str,
    write: fn(&Expr) -> TokenStream,
}

const SETTINGS: [Setting; 8] = [
    Setting {
        key: "keys",
        item: "cell_keys",
        write: |v| quote! { fn cell_keys() -> ::std::vec::Vec<&'static str> { ::std::vec::Vec::from(#v) } },
    },
    Setting {
        key: "capacity",
        item: "mailbox_capacity",
        write: |v| quote! { fn mailbox_capacity() -> usize { #v } },
    },
    Setting {
        key: "max_cells",
        item: "max_cells",
        write: |v| quote! { fn max_cells() -> usize { #v } },
    },
    Setting {
        key: "idle_ttl_seconds",
        item: "cell_idle_ttl",
        write: |v| {
            quote! {
                fn cell_idle_ttl() -> ::core::option::Option<::core::time::Duration> {
                    ::core::option::Option::Some(::core::time::Duration::from_secs(#v))
                }
            }
        },
    },
    Setting {
        key: "schema_version",
        item: "SCHEMA_VERSION",
        write: |v| quote! { const SCHEMA_VERSION: u32 = #v; },
    },
    Setting {
        key: "min_readable_schema_version",
        item: "MIN_READABLE_SCHEMA_VERSION",
        write: |v| quote! { const MIN_READABLE_SCHEMA_VERSION: u32 = #v; },
    },
    Setting {
        key: "compact_after_bytes",
        item: "compact_after_bytes",
        write: |v| quote! { fn compact_after_bytes() -> usize { #v } },
    },
    Setting {
        key: "max_timers_per_cell",
        item: "max_timers_per_cell",
        write: |v| quote! { fn max_timers_per_cell() -> usize { #v } },
    },
];

/// The flags the attribute derives, each from the hook that sets it.
const FLAGS: [(&str, &str); 4] = [
    ("COMPENSATE_OVERRIDDEN", "compensate"),
    ("CELLS_FOR_OVERRIDDEN", "cells_for"),
    ("MAX_CELLS_OVERRIDDEN", "max_cells"),
    ("MAILBOX_CAPACITY_OVERRIDDEN", "mailbox_capacity"),
];

/// The keys the short forms take to describe a relay and its entry
/// channel.
pub(crate) const RELAY_KEYS: [&str; 8] = [
    "id",
    "output",
    "input",
    "on_full",
    "description",
    "max_duration_ms",
    "run",
    "durable",
];

/// What the tasklet attribute keys say, whichever attribute carried them.
#[derive(Default)]
pub(crate) struct TaskletKeys {
    pub name: Option<LitStr>,
    settings: Vec<(&'static Setting, Expr)>,
    logical_clock: Option<Span>,
}

impl TaskletKeys {
    /// Take one key if it is a tasklet key; `Ok(false)` leaves it to the caller.
    pub(crate) fn take(&mut self, key: &str, meta: &Meta) -> syn::Result<bool> {
        if key == "logical_clock" {
            meta.require_path_only()?;
            self.logical_clock = Some(meta.span());
            return Ok(true);
        }
        let Some(setting) = SETTINGS.iter().find(|s| s.key == key) else {
            return Ok(false);
        };
        let value = meta.require_name_value()?.value.clone();
        self.settings.push((setting, value));
        Ok(true)
    }

    /// The setting given for the trait item `item`, if one was.
    fn setting_for(&self, item: &str) -> Option<&'static Setting> {
        self.settings
            .iter()
            .map(|(s, _)| *s)
            .find(|s| s.item == item)
    }
}

/// `#[flezha::tasklet(...)]`.
pub(crate) fn expand(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let keys = parse_keys(attr)?;
    let block = impl_target(item, "#[flezha::tasklet]")?;
    let name = keys.name.clone().ok_or_else(|| {
        syn::Error::new(
            Span::call_site(),
            "#[flezha::tasklet] needs a `name`: the tasklet's name within its relay, \
             which its saved state is keyed by",
        )
    })?;
    complete(&keys, &name, block)
}

fn parse_keys(attr: TokenStream) -> syn::Result<TaskletKeys> {
    let mut keys = TaskletKeys::default();
    for meta in parse_metas(attr)? {
        let key = key_of(&meta)?;
        if key == "name" {
            keys.name = Some(lit_str(&meta.require_name_value()?.value)?.clone());
        } else if RELAY_KEYS.contains(&key.as_str()) {
            return Err(syn::Error::new_spanned(
                meta.path(),
                format!(
                    "#[flezha::tasklet] declares a tasklet; `{key}` belongs to a relay \
                     declaration: write the short form as `#[flezha::relay(..)]`, or wire \
                     it in a full form's topology"
                ),
            ));
        } else if !keys.take(&key, &meta)? {
            return Err(unknown_key(&meta, &key, "#[flezha::tasklet]", &["name"]));
        }
    }
    Ok(keys)
}

pub(crate) fn parse_metas(attr: TokenStream) -> syn::Result<Punctuated<Meta, Token![,]>> {
    use syn::parse::Parser;
    Punctuated::<Meta, Token![,]>::parse_terminated.parse2(attr)
}

pub(crate) fn key_of(meta: &Meta) -> syn::Result<String> {
    Ok(meta
        .path()
        .get_ident()
        .ok_or_else(|| syn::Error::new_spanned(meta.path(), "expected a simple attribute key"))?
        .to_string())
}

/// An unknown key, refused with every key the attribute takes.
pub(crate) fn unknown_key(meta: &Meta, key: &str, attribute: &str, own: &[&str]) -> syn::Error {
    if key == "cell_key" {
        return syn::Error::new_spanned(
            meta.path(),
            format!(
                "{attribute}: `cell_key` no longer exists; declare the Cell key on the wiring \
                 instead: `.key_by(f)` on the stream that feeds this tasklet"
            ),
        );
    }
    let accepted: Vec<&str> = own
        .iter()
        .copied()
        .chain(SETTINGS.iter().map(|s| s.key))
        .chain(["logical_clock"])
        .collect();
    syn::Error::new_spanned(
        meta.path(),
        format!(
            "{attribute}: unknown attribute `{key}`; accepted: {}",
            accepted.join(", ")
        ),
    )
}

pub(crate) fn lit_str(expr: &Expr) -> syn::Result<&LitStr> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(s), ..
        }) => Ok(s),
        // A `macro_rules!` fragment arrives wrapped in an invisible group.
        Expr::Group(group) => lit_str(&group.expr),
        _ => Err(syn::Error::new_spanned(expr, "expected a string literal")),
    }
}

/// The `impl Tasklet for T` block an attribute sits on. A function is refused
/// towards the `impl` block or the short forms, and a second completing
/// attribute on the same block is refused because both would name it.
pub(crate) fn impl_target(item: TokenStream, attribute: &str) -> syn::Result<ItemImpl> {
    let block = match syn::parse2::<Item>(item)? {
        Item::Impl(block) => block,
        Item::Fn(func) => {
            return Err(syn::Error::new_spanned(
                &func.sig,
                format!(
                    "{attribute} sits on an `impl Tasklet for YourType` block; a one-tasklet \
                     relay written as a function takes `#[flezha::relay(..)]`"
                ),
            ))
        }
        other => {
            return Err(syn::Error::new_spanned(
                other,
                format!("{attribute} expects an `impl Tasklet for YourType` block"),
            ))
        }
    };
    let is_tasklet = block
        .trait_
        .as_ref()
        .is_some_and(|(_, path, _)| path.segments.last().is_some_and(|s| s.ident == "Tasklet"));
    if !is_tasklet {
        return Err(syn::Error::new_spanned(
            &block.self_ty,
            format!("{attribute} expects an `impl Tasklet for YourType` block"),
        ));
    }
    if let Some(other) = block.attrs.iter().find(|a| {
        a.path()
            .segments
            .last()
            .is_some_and(|s| COMPLETING_ATTRIBUTES.contains(&s.ident.to_string().as_str()))
    }) {
        return Err(syn::Error::new_spanned(
            other,
            format!(
                "{attribute} already completes this block and names its tasklet; a block takes \
                 one of `#[flezha::tasklet]` and `#[flezha::relay]`"
            ),
        ));
    }
    Ok(block)
}

/// The names of the items a block writes.
fn written(block: &ItemImpl) -> Vec<(String, Span)> {
    block
        .items
        .iter()
        .filter_map(|item| match item {
            ImplItem::Type(t) => Some((t.ident.to_string(), t.ident.span())),
            ImplItem::Const(c) => Some((c.ident.to_string(), c.ident.span())),
            ImplItem::Fn(f) => Some((f.sig.ident.to_string(), f.sig.ident.span())),
            _ => None,
        })
        .collect()
}

/// Complete `block` with every item the keys and the written hooks decide.
pub(crate) fn complete(
    keys: &TaskletKeys,
    name: &LitStr,
    mut block: ItemImpl,
) -> syn::Result<TokenStream> {
    let written = written(&block);
    let writes = |item: &str| written.iter().any(|(w, _)| w == item);
    refuse_written(keys, &written)?;
    let clock = clock(keys, &written)?;
    if (keys.setting_for("cell_keys").is_some() || writes("cell_keys")) != writes("cells_for") {
        return Err(syn::Error::new(
            block.self_ty.span(),
            "#[flezha::tasklet] a static cell needs both its keys (`keys = [..]` or a written \
             `fn cell_keys`) and a written `fn cells_for`",
        ));
    }
    let flags = FLAGS.iter().filter_map(|(flag, hook)| {
        let flag = syn::Ident::new(flag, Span::call_site());
        (writes(hook) || keys.setting_for(hook).is_some())
            .then(|| quote! { const #flag: bool = true; })
    });
    let settings = keys.settings.iter().map(|(s, value)| (s.write)(value));
    let defaults = defaults()
        .into_iter()
        .filter(|(item, _)| !writes(item))
        .map(|(_, t)| t);
    let added: Vec<TokenStream> = [
        quote! { const NAME: &'static str = #name; },
        quote! { const CLOCK: ::flezha_runner::ClockKind = ::flezha_runner::ClockKind::#clock; },
    ]
    .into_iter()
    .chain(flags)
    .chain(settings)
    .chain(defaults)
    .collect();
    for tokens in added {
        block.items.push(syn::parse2(tokens)?);
    }
    Ok(quote! { #block })
}

/// The items a block may leave out, each with what stands in for it.
fn defaults() -> [(&'static str, TokenStream); 4] {
    [
        ("Output", quote! { type Output = (); }),
        ("State", quote! { type State = (); }),
        (
            "Change",
            quote! { type Change = ::flezha_runner::NoChange; },
        ),
        (
            "init_state",
            quote! {
                fn init_state(_: ::core::option::Option<&str>) -> Self::State {
                    ::core::default::Default::default()
                }
            },
        ),
    ]
}

/// An item the attribute writes may not also be written by the block.
fn refuse_written(keys: &TaskletKeys, written: &[(String, Span)]) -> syn::Result<()> {
    for (item, span) in written {
        let reason = match item.as_str() {
            "NAME" => "the attribute's name key sets it".to_string(),
            "CLOCK" => "the attribute sets it: event time from a written `event_time`, \
                        the logical clock from `logical_clock`, the wall clock otherwise"
                .to_string(),
            flag if FLAGS.iter().any(|(f, _)| *f == flag) => {
                "the attribute derives it from the hooks the block writes".to_string()
            }
            _ => match keys.setting_for(item) {
                Some(setting) => format!("the `{}` key sets it", setting.key),
                None => continue,
            },
        };
        return Err(syn::Error::new(
            *span,
            format!("#[flezha::tasklet]: `{item}` is written twice: {reason}; remove one"),
        ));
    }
    Ok(())
}

fn clock(keys: &TaskletKeys, written: &[(String, Span)]) -> syn::Result<syn::Ident> {
    let event_time = written.iter().find(|(w, _)| w == "event_time");
    let clock =
        match (keys.logical_clock, event_time) {
            (Some(span), Some(_)) => return Err(syn::Error::new(
                span,
                "#[flezha::tasklet]: `logical_clock` and a written `event_time` are two clocks; \
                 a tasklet has one",
            )),
            (Some(_), None) => "Logical",
            (None, Some(_)) => "EventTime",
            (None, None) => "Wall",
        };
    Ok(syn::Ident::new(clock, Span::call_site()))
}
