//! Shared helpers for the Flezha proc-macros.

use syn::{GenericArgument, Ident, PathArguments, Type};

/// If `ty` is `Name<T, ..>` (matching the last path segment), return `T` —
/// e.g. `Vec<U>` → `U`, `Result<O, E>` → `O`.
pub fn first_generic_arg<'a>(ty: &'a Type, name: &str) -> Option<&'a Type> {
    let Type::Path(tp) = ty else { return None };
    let seg = tp.path.segments.last()?;
    if seg.ident != name {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    args.args.iter().find_map(|a| match a {
        GenericArgument::Type(t) => Some(t),
        _ => None,
    })
}

/// `do_nothing` → `DoNothing`. Used for the generated shard type name and for
/// `Set<Field>` / `Push<Field>` change-variant names.
pub fn snake_ident_to_pascal(ident: &Ident) -> Ident {
    let pascal = snake_to_pascal(&ident.to_string());
    Ident::new(&pascal, ident.span())
}

/// `do_nothing` → `DoNothing`.
pub fn snake_to_pascal(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut upper_next = true;
    for c in s.chars() {
        if c == '_' {
            upper_next = true;
        } else if upper_next {
            out.extend(c.to_uppercase());
            upper_next = false;
        } else {
            out.push(c);
        }
    }
    out
}
