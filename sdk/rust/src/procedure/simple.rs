//! The simplified [`Procedure`] tier: declare what a procedure is *about*
//! (Input, Output, State) and inherit the rest.
//!
//! Stable Rust has no associated-type defaults, so a `Procedure` impl spells
//! all six even when three of them are the near-universal values. This trait is
//! that shorthand, lifted to `Procedure` by a blanket impl — so everything
//! downstream still sees exactly one runtime-facing trait.
//!
//! The shorthand carries only what it declares. A procedure that needs any
//! other `Procedure` item — a non-default `Clock`, a typed `Config`, a
//! hand-written `Change`, static entity keys, [`Procedure::on_timer`] or
//! [`Procedure::compensate`] — implements `Procedure` directly. `on_timer` is
//! the one worth naming: registering a timer without it is a runtime panic
//! rather than a build error, and this tier cannot override it. A type cannot
//! sit in both tiers — the blanket impl and a direct impl overlap, which the
//! compiler refuses.

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::entity::restore::RestoreInitPayload;
use crate::procedure::{
    DerivedState, Procedure, ProcedureCtx, ProcedureError, StateHandle, WallClock,
};

/// A [`Procedure`] over the common defaults: [`WallClock`], no typed config,
/// and the change type derived from the state.
///
/// ```
/// use flezha_runner::procedure::{ProcedureCtx, ProcedureError, SimpleProcedure};
/// use flezha_runner::state::StateHandle;
///
/// #[derive(Default, serde::Serialize, serde::Deserialize, flezha_runner::State)]
/// struct Totals {
///     count: u64,
/// }
///
/// #[derive(Default)]
/// struct CountTrades;
///
/// impl SimpleProcedure for CountTrades {
///     type Input = f64;
///     type Output = u64;
///     type State = Totals;
///
///     fn name() -> &'static str {
///         "count_trades"
///     }
///
///     fn init_state(_entity_key: Option<&str>) -> Totals {
///         Totals::default()
///     }
///
///     fn run(
///         &mut self,
///         _input: f64,
///         mut state: StateHandle<'_, Self>,
///         ctx: &mut ProcedureCtx<'_>,
///     ) -> Result<(), ProcedureError> {
///         state.set_count(state.count + 1);
///         ctx.emit(state.count)
///     }
/// }
/// ```
pub trait SimpleProcedure: Send + Sync + 'static + Default {
    /// Per-entity input value. Same bounds as [`Procedure::Input`].
    type Input: Send + Sync + 'static + Clone;

    /// Per-entity output value. Same bounds as [`Procedure::Output`].
    type Output: Send + Sync + 'static;

    /// Per-entity state. Bound on [`DerivedState`] — the change type comes
    /// from it, which is what removes the `type Change` declaration. A
    /// stateless procedure declares `type State = ()`, whose derived change is
    /// [`NoChange`](crate::procedure::NoChange).
    type State: Send + Sync + 'static + Serialize + DeserializeOwned + DerivedState;

    /// Stable identifier within the StreamOperative class. See [`Procedure::name`].
    fn name() -> &'static str;

    /// Per-entity state factory. See [`Procedure::init_state`] — the restore
    /// payload it also receives is applied by the runtime either way, so this
    /// tier does not carry it.
    fn init_state(entity_key: Option<&str>) -> Self::State;

    /// Compute. See [`Procedure::run`], whose contract — including determinism
    /// — applies unchanged.
    fn run(
        &mut self,
        input: <Self as SimpleProcedure>::Input,
        state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError>
    where
        Self: Procedure;
}

impl<T: SimpleProcedure> Procedure for T {
    type Input = <T as SimpleProcedure>::Input;
    type Output = <T as SimpleProcedure>::Output;
    type State = <T as SimpleProcedure>::State;
    type Change = <<T as SimpleProcedure>::State as DerivedState>::Change;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        <T as SimpleProcedure>::name()
    }

    fn init_state(
        entity_key: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> Self::State {
        <T as SimpleProcedure>::init_state(entity_key)
    }

    fn apply_change(state: &mut Self::State, change: &Self::Change) {
        <<T as SimpleProcedure>::State as DerivedState>::apply_change(state, change);
    }

    fn run(
        &mut self,
        input: Self::Input,
        state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        <T as SimpleProcedure>::run(self, input, state, ctx)
    }
}
