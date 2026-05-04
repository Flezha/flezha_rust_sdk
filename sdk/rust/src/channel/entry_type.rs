//! Sealed marker trait restricting entry-channel types to `Vec<u8>` and
//! `String` per umbrella Decision 12.

mod sealed {
    pub trait Sealed {}
    impl Sealed for Vec<u8> {}
    impl Sealed for String {}
}

/// Allowed payload types for entry channels. Closed in v1: `Vec<u8>` and
/// `String`. Adding more types is a future change.
pub trait EntryChannelType: Send + Sync + 'static + sealed::Sealed {}

impl EntryChannelType for Vec<u8> {}
impl EntryChannelType for String {}
