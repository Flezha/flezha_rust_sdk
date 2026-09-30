//! Where the contract set a suite reads its shared fixtures from is.

/// A member of the contract set, resolved in the order every SDK uses:
/// `$FLEZHA_CONTRACT_DIR`, else the nearest enclosing tree that carries the
/// set's manifest, else `contract/` beside this crate. The enclosing tree comes
/// first so a stale copy fetched into a crate inside the core's checkout never
/// shadows the live files. None resolving fails naming all three, never skips.
pub fn contract_file(member: &str) -> std::path::PathBuf {
    let configured = std::env::var_os("FLEZHA_CONTRACT_DIR").map(std::path::PathBuf::from);
    contract_root(configured, std::path::Path::new(env!("CARGO_MANIFEST_DIR"))).join(member)
}

pub const MANIFEST: &str = "test/fixtures/contract.txt";

/// Where the contract set is for a crate at `package`.
pub fn contract_root(
    configured: Option<std::path::PathBuf>,
    package: &std::path::Path,
) -> std::path::PathBuf {
    let beside = package.join("contract");
    let enclosing = package
        .ancestors()
        .find(|dir| dir.join(MANIFEST).is_file())
        .map(std::path::Path::to_path_buf);
    [configured.clone(), enclosing, Some(beside.clone())]
        .into_iter()
        .flatten()
        .find(|root| root.join(MANIFEST).is_file())
        .unwrap_or_else(|| {
            panic!(
                "the contract set is nowhere this suite looked: $FLEZHA_CONTRACT_DIR ({}), \
                 no directory above {} carries {MANIFEST}, and {}. Unpack \
                 flezha-fixtures-<version>.tar.gz at contract/, or set FLEZHA_CONTRACT_DIR.",
                configured.map_or("unset".to_string(), |dir| dir.display().to_string()),
                package.display(),
                beside.display(),
            )
        })
}
