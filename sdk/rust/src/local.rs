//! Running a declared operative graph in this process, with no controller.
//!
//! [`LocalOperative::start`] takes the operative type you deploy, runs the
//! `channels()` graph it already declares, and drives it on the crate's own
//! poll workers — the same ones a started runner uses. Nothing about the
//! class changes: no local-mode member, no second declaration.
//!
//! ```ignore
//! let run = LocalOperative::start::<WordCount>()?;
//! run.on_sink("totals", |output| println!("{:?}", output.payload))?;
//! run.feed("lines", "the quick brown fox")?;
//! run.run_until_idle(Duration::from_secs(5))?;
//! ```
//!
//! State is held in memory and nothing is durable. Durability, plugins,
//! placement and migration, the dead-letter queue, observability export,
//! authz and multi-runner scale all need a controller; the run says so on
//! stderr every time it starts. `docs/quickstart.md` is the walk-through.

use std::sync::Arc;
use std::time::Duration;

use flz_core::local::LocalRun;
pub use flz_core::local::{SinkOutput, NON_DURABILITY_NOTICE};

use crate::operative::StreamOperative;
use crate::runner_handle::FlezhaError;

/// One operative graph running in this process. Dropping it stops the run.
pub struct LocalOperative {
    run: LocalRun,
}

impl LocalOperative {
    /// Register `R`'s declared graph and start it locally. Refuses a
    /// declaration that needs a controller, naming the capability.
    pub fn start<R: StreamOperative>() -> Result<Self, FlezhaError> {
        crate::register_stream_class::<R>()?;
        crate::operative::poll_pool::ensure_poll_workers();
        Ok(LocalOperative {
            run: LocalRun::start(R::factory_id(), &format!("{}-local", R::factory_id()))?,
        })
    }

    /// Deliver every value released on the sink `channel` to `callback`, in
    /// release order. The bytes are what the channel's serializer produced
    /// and the metadata is what a deployed run would have put on the wire.
    pub fn on_sink(
        &self,
        channel: &str,
        callback: impl Fn(SinkOutput) + Send + Sync + 'static,
    ) -> Result<(), FlezhaError> {
        self.run.on_sink(channel, Arc::new(callback))?;
        Ok(())
    }

    /// Feed one value's wire bytes to a declared entry channel — the same
    /// bytes a controller would forward, decoded by the channel's own
    /// parser. Blocks while the consumer's mailbox is full.
    pub fn feed(&self, channel: &str, payload: impl AsRef<[u8]>) -> Result<(), FlezhaError> {
        self.run.feed(channel, payload.as_ref().to_vec())?;
        Ok(())
    }

    /// Block until the graph has nothing left to do, or `budget` elapses —
    /// which is reported rather than passed off as done.
    pub fn run_until_idle(&self, budget: Duration) -> Result<(), FlezhaError> {
        self.run.run_until_idle(budget)?;
        Ok(())
    }
}
