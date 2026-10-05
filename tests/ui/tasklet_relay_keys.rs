//! `sink`, `input`, `kind` and `on_full` declare a relay, so a tasklet
//! refuses them, and a short form's kind is its attribute's name, never a key.
use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

macro_rules! refused {
    ($ty:ident, $($key:tt)*) => {
        #[derive(Default)]
        struct $ty;

        #[tasklet(name = "t", $($key)*)]
        impl Tasklet for $ty {
            type Input = String;

            fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
                Ok(())
            }
        }
    };
}

refused!(A, sink = "echo");
refused!(B, input = "prices");
refused!(C, kind = "flow");
refused!(E, on_full = drop_oldest);

#[flezha_runner::relay(id = "d", kind = "flow", sink = "echo")]
fn d(input: String) -> Vec<u8> {
    input.into_bytes()
}

fn main() {}
