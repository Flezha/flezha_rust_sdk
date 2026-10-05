//! Shared helpers for the Flezha proc-macros.

use syn::{GenericArgument, Ident, PathArguments, Type};

/// The type arguments of `ty` when its last path segment is `name`:
/// `Vec<U>` → `U`, `Result<O, E>` → `O, E`, `HashMap<K, V>` → `K, V`. Empty
/// for any other shape, so a caller reads exactly the arity it needs.
pub fn generic_args<'a>(ty: &'a Type, name: &str) -> impl Iterator<Item = &'a Type> {
    let args = match ty {
        Type::Path(tp) => tp
            .path
            .segments
            .last()
            .filter(|seg| seg.ident == name)
            .and_then(|seg| match &seg.arguments {
                PathArguments::AngleBracketed(args) => Some(&args.args),
                _ => None,
            }),
        _ => None,
    };
    args.into_iter().flatten().filter_map(|a| match a {
        GenericArgument::Type(t) => Some(t),
        _ => None,
    })
}

/// `do_nothing` → `DoNothing`: the relay type a function short form
/// declares.
pub fn snake_ident_to_pascal(ident: &Ident) -> Ident {
    let mut out = String::new();
    let mut upper_next = true;
    for c in ident.to_string().chars() {
        if c == '_' {
            upper_next = true;
        } else if upper_next {
            out.extend(c.to_uppercase());
            upper_next = false;
        } else {
            out.push(c);
        }
    }
    Ident::new(&out, ident.span())
}
