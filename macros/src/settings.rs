//! The settings tree as the short form writes it, lowered to one
//! `fn settings()` item:
//!
//! ```text
//! run = steps(each = chained, on_failure = keep_progress)
//! run = flow(exactly = effectively_once(ack = checkpoint(every = 100, max_delay_ms = 1000)))
//! durable = local(sleep_after = 2000)   // milliseconds, or `false`
//! durable = remote(keep_finished = 24)  // hours, `true` or `false`
//! durable = false
//! max_duration_ms = 600000
//! description = "what it is for"
//! ```
//!
//! A setting written under a parent that does not carry it is a compile
//! error naming both.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Expr, ExprLit, Lit};

/// A branch as written: its name and its `member = value` pairs.
struct Branch<'a> {
    name: String,
    members: Vec<(String, &'a Expr, &'a Expr)>,
}

fn branch<'a>(setting: &str, expr: &'a Expr, names: &[&str]) -> syn::Result<Branch<'a>> {
    let refuse = || {
        syn::Error::new_spanned(
            expr,
            format!("`{setting}` must be one of: {}", names.join(", ")),
        )
    };
    let (head, args) = match expr {
        Expr::Group(group) => return branch(setting, &group.expr, names),
        Expr::Path(path) => (path.path.get_ident().ok_or_else(refuse)?.to_string(), None),
        Expr::Call(call) => match &*call.func {
            Expr::Path(path) => (
                path.path.get_ident().ok_or_else(refuse)?.to_string(),
                Some(&call.args),
            ),
            _ => return Err(refuse()),
        },
        _ => return Err(refuse()),
    };
    if !names.contains(&head.as_str()) {
        return Err(refuse());
    }
    let mut members = Vec::new();
    for arg in args.into_iter().flatten() {
        let Expr::Assign(assign) = arg else {
            return Err(syn::Error::new_spanned(
                arg,
                format!("`{head}` takes `member = value` pairs"),
            ));
        };
        let key = match &*assign.left {
            Expr::Path(path) => path.path.get_ident().map(|i| i.to_string()),
            _ => None,
        }
        .ok_or_else(|| syn::Error::new_spanned(&assign.left, "expected a setting name"))?;
        members.push((key, &*assign.left, &*assign.right));
    }
    Ok(Branch {
        name: head,
        members,
    })
}

impl Branch<'_> {
    /// Refuse any member outside `known`, naming it and this branch.
    fn only(&self, known: &[&str]) -> syn::Result<()> {
        match self
            .members
            .iter()
            .find(|(key, ..)| !known.contains(&key.as_str()))
        {
            Some((key, at, _)) => Err(syn::Error::new_spanned(
                at,
                format!("`{key}` is not a setting of `{}`", self.name),
            )),
            None => Ok(()),
        }
    }

    fn get(&self, key: &str) -> Option<&Expr> {
        self.members
            .iter()
            .find(|(k, ..)| k == key)
            .map(|(.., value)| *value)
    }
}

/// A bare choice among `choices`, as the path `ty::Variant`.
fn choice(
    setting: &str,
    expr: &Expr,
    choices: &[(&str, &str)],
    ty: TokenStream,
) -> syn::Result<TokenStream> {
    let names: Vec<&str> = choices.iter().map(|(name, _)| *name).collect();
    let chosen = branch(setting, expr, &names)?;
    chosen.only(&[])?;
    let (_, variant) = choices
        .iter()
        .find(|(name, _)| *name == chosen.name)
        .expect("checked");
    let variant = syn::Ident::new(variant, proc_macro2::Span::call_site());
    Ok(quote! { #ty::#variant })
}

fn integer(expr: &Expr) -> syn::Result<TokenStream> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Int(n), ..
        }) => Ok(quote! { #n }),
        Expr::Group(group) => integer(&group.expr),
        _ => Err(syn::Error::new_spanned(expr, "expected an integer literal")),
    }
}

fn optional(value: Option<&Expr>) -> syn::Result<TokenStream> {
    match value {
        Some(expr) => {
            let n = integer(expr)?;
            Ok(quote! { ::core::option::Option::Some(#n) })
        }
        None => Ok(quote! { ::core::option::Option::None }),
    }
}

/// `run = flow | steps`, with their members.
pub(crate) fn run(expr: &Expr) -> syn::Result<TokenStream> {
    let written = branch("run", expr, &["flow", "steps"])?;
    if written.name == "flow" {
        written.only(&["exactly"])?;
        let exactly = match written.get("exactly") {
            Some(exactly) => self::exactly(exactly)?,
            None => quote! { ::flezha_runner::Exactly::AtLeastOnce },
        };
        return Ok(quote! { ::flezha_runner::Run::Flow { exactly: #exactly } });
    }
    written.only(&["each", "on_failure"])?;
    let each = match written.get("each") {
        Some(each) => choice(
            "each",
            each,
            &[
                ("once", "Once"),
                ("per_input", "PerInput"),
                ("chained", "Chained"),
            ],
            quote! { ::flezha_runner::Each },
        )?,
        None => quote! { ::flezha_runner::Each::Once },
    };
    let on_failure = match written.get("on_failure") {
        Some(on_failure) => choice(
            "on_failure",
            on_failure,
            &[("undo_all", "UndoAll"), ("keep_progress", "KeepProgress")],
            quote! { ::flezha_runner::OnFailure },
        )?,
        None => quote! { ::flezha_runner::OnFailure::UndoAll },
    };
    Ok(quote! { ::flezha_runner::Run::Steps { each: #each, on_failure: #on_failure } })
}

fn exactly(expr: &Expr) -> syn::Result<TokenStream> {
    let written = branch("exactly", expr, &["at_least_once", "effectively_once"])?;
    if written.name == "at_least_once" {
        written.only(&[])?;
        return Ok(quote! { ::flezha_runner::Exactly::AtLeastOnce });
    }
    written.only(&["ack", "correction_ms", "beyond_window"])?;
    let ack = match written.get("ack") {
        Some(ack) => self::ack(ack)?,
        None => quote! { ::flezha_runner::AckGranularity::PerInput },
    };
    let correction_ms = optional(written.get("correction_ms"))?;
    let beyond_window = match written.get("beyond_window") {
        Some(beyond) => choice(
            "beyond_window",
            beyond,
            &[("process_as_arrival", "ProcessAsArrival"), ("drop", "Drop")],
            quote! { ::flezha_runner::BeyondWindowPolicy },
        )?,
        None => quote! { ::flezha_runner::BeyondWindowPolicy::ProcessAsArrival },
    };
    Ok(quote! {
        ::flezha_runner::Exactly::EffectivelyOnce {
            ack: #ack,
            correction_ms: #correction_ms,
            beyond_window: #beyond_window,
        }
    })
}

fn ack(expr: &Expr) -> syn::Result<TokenStream> {
    let written = branch("ack", expr, &["per_input", "checkpoint"])?;
    if written.name == "per_input" {
        written.only(&[])?;
        return Ok(quote! { ::flezha_runner::AckGranularity::PerInput });
    }
    written.only(&["every", "max_delay_ms", "outputs"])?;
    let every = optional(written.get("every"))?;
    let max_delay_ms = optional(written.get("max_delay_ms"))?;
    let outputs = match written.get("outputs") {
        Some(outputs) => choice(
            "outputs",
            outputs,
            &[
                ("as_produced", "AsProduced"),
                ("at_checkpoint", "AtCheckpoint"),
            ],
            quote! { ::flezha_runner::CheckpointOutputs },
        )?,
        None => quote! { ::flezha_runner::CheckpointOutputs::AsProduced },
    };
    Ok(quote! {
        ::flezha_runner::AckGranularity::Checkpoint(::flezha_runner::CheckpointPolicy {
            every: #every,
            outputs: #outputs,
            max_delay_ms: #max_delay_ms,
        })
    })
}

/// `durable = false | local | remote`, the last two with `sleep_after` and
/// `keep_finished`.
pub(crate) fn durable(expr: &Expr) -> syn::Result<TokenStream> {
    if let Expr::Lit(ExprLit {
        lit: Lit::Bool(b), ..
    }) = expr
    {
        if !b.value {
            return Ok(quote! { ::flezha_runner::Durable::Memory });
        }
    }
    let written = branch("durable", expr, &["local", "remote"])?;
    written.only(&["sleep_after", "keep_finished"])?;
    let level = syn::Ident::new(
        if written.name == "local" {
            "Local"
        } else {
            "Remote"
        },
        proc_macro2::Span::call_site(),
    );
    let sleep_after = match written.get("sleep_after") {
        None => quote! { ::flezha_runner::SleepAfter::RunDefault },
        Some(Expr::Lit(ExprLit {
            lit: Lit::Bool(b), ..
        })) if !b.value => {
            quote! { ::flezha_runner::SleepAfter::Never }
        }
        Some(ms) => {
            let ms = integer(ms)?;
            quote! { ::flezha_runner::SleepAfter::After(::core::time::Duration::from_millis(#ms)) }
        }
    };
    let keep_finished = keep_finished(written.get("keep_finished"))?;
    Ok(quote! {
        ::flezha_runner::Durable::#level { sleep_after: #sleep_after, keep_finished: #keep_finished }
    })
}

/// `keep_finished = hours | true | false`; unwritten keeps nothing.
fn keep_finished(expr: Option<&Expr>) -> syn::Result<TokenStream> {
    Ok(match expr {
        None => quote! { ::flezha_runner::KeepFinished::Nothing },
        Some(Expr::Lit(ExprLit {
            lit: Lit::Bool(b), ..
        })) => {
            if b.value {
                quote! { ::flezha_runner::KeepFinished::ControllerWindow }
            } else {
                quote! { ::flezha_runner::KeepFinished::Nothing }
            }
        }
        Some(hours) => {
            let hours = integer(hours)?;
            quote! { ::flezha_runner::KeepFinished::Hours(#hours) }
        }
    })
}

/// The settings the short form wrote, as struct fields.
#[derive(Default)]
pub(crate) struct Written {
    pub fields: Vec<TokenStream>,
}

impl Written {
    /// Take one settings key; `Ok(false)` leaves it to the caller.
    pub(crate) fn take(&mut self, key: &str, value: &Expr) -> syn::Result<bool> {
        let field = match key {
            "run" => {
                let run = run(value)?;
                quote! { run: #run }
            }
            "durable" => {
                let durable = durable(value)?;
                quote! { durable: #durable }
            }
            "max_duration_ms" => {
                let ms = integer(value)?;
                quote! {
                    max_duration: ::core::option::Option::Some(
                        ::core::time::Duration::from_millis(#ms)
                    )
                }
            }
            "description" => {
                let text = crate::tasklet::lit_str(value)?;
                quote! { description: #text }
            }
            _ => return Ok(false),
        };
        self.fields.push(field);
        Ok(true)
    }

    /// The `fn settings()` item, empty when every setting keeps its default.
    pub(crate) fn item(&self) -> TokenStream {
        if self.fields.is_empty() {
            return TokenStream::new();
        }
        let fields = &self.fields;
        quote! {
            fn settings() -> ::flezha_runner::RelaySettings {
                ::flezha_runner::RelaySettings {
                    #(#fields,)*
                    ..::flezha_runner::RelaySettings::default()
                }
            }
        }
    }
}
