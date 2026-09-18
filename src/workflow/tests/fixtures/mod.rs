//! The workflow classes the registration tests and the driver scenarios
//! are written against, by owner.

/// A stateless `String -> String` procedure whose `run` is the given body.
macro_rules! string_proc {
    ($ty:ident, $name:literal, |$input:ident, $ctx:ident| $body:expr) => {
        #[derive(Default)]
        pub(crate) struct $ty;
        impl crate::procedure::Procedure for $ty {
            type Input = String;
            type Output = String;
            type State = ();
            type Change = crate::procedure::change::NoChange;
            type Clock = crate::procedure::clock::WallClock;
            type Config = ();
            fn name() -> &'static str {
                $name
            }
            fn init_state(
                _: Option<&str>,
                _: Option<crate::entity::restore::RestoreInitPayload<'_>>,
            ) {
            }
            fn run(
                &mut self,
                $input: String,
                _state: crate::procedure::StateHandle<'_, Self>,
                $ctx: &mut crate::procedure::ProcedureCtx<'_>,
            ) -> Result<(), crate::procedure::ProcedureError> {
                $body
            }
        }
    };
}

mod fan_in;
mod lifecycle;
mod outbox;
mod retry;
mod saga;
mod structured;

pub(crate) use fan_in::*;
pub(crate) use lifecycle::*;
pub(crate) use outbox::*;
pub(crate) use retry::*;
pub(crate) use saga::*;
pub(crate) use structured::*;
