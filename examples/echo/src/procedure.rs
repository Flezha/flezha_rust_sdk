use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};

/// A processing stage of the operative's graph, run once per message. This one
/// prefixes the input and emits the result.
///
/// Messages that carry the same key are never handled concurrently, so
/// per-entity state needs no locking. Messages for different keys are handled
/// concurrently, on this runner or another.
#[derive(Default)]
pub struct EchoProcedure;

impl Procedure for EchoProcedure {
    type Input = String;
    type Output = String;
    /// Nothing is kept between messages.
    type State = ();
    /// No state changes are recorded, so nothing is journaled or snapshotted.
    type Change = NoChange;
    type Clock = WallClock;
    /// No configuration.
    type Config = ();

    /// The procedure's stable identity. State is filed under it, so renaming
    /// it later declares a different procedure rather than renaming this one.
    fn name() -> &'static str {
        "echo"
    }

    /// The state for an entity seen for the first time.
    fn init_state(_entity_key: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}

    /// Where the emitted value goes is not decided here. The operative decides
    /// that, so the graph can be rewired without touching this procedure.
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(format!("echo: {input}"))
    }
}
