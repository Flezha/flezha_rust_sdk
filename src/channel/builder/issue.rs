//! The refusal vocabulary lives in `flz_core::host::issue`: one name and one
//! sentence per shape a declared topology can take that the runtime cannot
//! honour, shared by every SDK. This module is the re-export the typed
//! builder's callers read it through.

pub use flz_core::host::TopologyIssue;
