//! The bounded classes the registration tests and the driver scenarios
//! are written against, by owner.

/// A stateless `String -> String` tasklet whose `run` is the given body.
macro_rules! string_task {
    ($ty:ident, $name:literal, |$input:ident, $ctx:ident| $body:expr) => {
        #[derive(Default)]
        pub(crate) struct $ty;
        #[flezha_runner::tasklet(name = $name)]
        impl crate::tasklet::Tasklet for $ty {
            type Input = String;
            type Output = String;
            fn run(
                &mut self,
                $input: String,
                $ctx: &mut crate::tasklet::Ctx<'_, Self>,
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
