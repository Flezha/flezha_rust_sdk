//! The workflow classes the registration tests and the driver scenarios
//! are written against, by owner.

/// A stateless `String -> String` tasklet whose `run` is the given body.
macro_rules! string_task {
    ($ty:ident, $name:literal, |$input:ident, $ctx:ident| $body:expr) => {
        #[derive(Default)]
        pub(crate) struct $ty;
        impl crate::tasklet::Tasklet for $ty {
            type Input = String;
            type Output = String;
            type State = ();
            type Change = crate::tasklet::change::NoChange;
            type Clock = crate::tasklet::clock::WallClock;
            type Config = ();
            fn name() -> &'static str {
                $name
            }
            fn init_state(
                _: Option<&str>,
                _: Option<crate::cell::restore::RestoreInitPayload<'_>>,
            ) {
            }
            fn run(
                &mut self,
                $input: String,
                _state: crate::tasklet::StateHandle<'_, Self>,
                $ctx: &mut crate::tasklet::TaskletCtx<'_>,
            ) -> Result<(), crate::tasklet::TaskletError> {
                $body
            }
        }
    };
}

mod entrance;
mod fan_in;
mod keyed;
mod lifecycle;
mod outbox;
mod retry;
mod saga;
mod structured;
mod timers;

pub(crate) use entrance::*;
pub(crate) use fan_in::*;
pub(crate) use keyed::*;
pub(crate) use lifecycle::*;
pub(crate) use outbox::*;
pub(crate) use retry::*;
pub(crate) use saga::*;
pub(crate) use structured::*;
pub(crate) use timers::*;
