#[flezha_runner::relay(factory_id = "x", output = "s")]
fn f(a: String) {
    let _ = a;
}

#[derive(Default)]
struct G;

#[flezha_runner::relay(factory_id = "g", output = "s")]
impl flezha_runner::Tasklet for G {
    type Input = String;

    fn run(
        &mut self,
        _: String,
        _: &mut flezha_runner::Ctx<'_, Self>,
    ) -> Result<(), flezha_runner::TaskletError> {
        Ok(())
    }
}

fn main() {}
