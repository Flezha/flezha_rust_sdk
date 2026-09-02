//! Idle observation for entry channels: the policy half of the input-liveness
//! signal `flz_core::entry_input` records.
//!
//! The core knows when a frame last arrived; only the application knows
//! whether the silence since then is anomalous — a quiet overnight market is
//! not a dead feed. So the SDK ships no default threshold and no default
//! action: [`FlezhaRunner::on_entry_idle`](crate::FlezhaRunner::on_entry_idle)
//! takes both from the caller and reports episodes.
//!
//! The episode latch itself is the core's (`flz_core::entry_input::IdleWatch`),
//! shared with every host over the C ABI; this module owns the registrations,
//! the tick and the delivery. Evaluation runs on one background tick per
//! runner, never on the delivery path, so a slow or panicking observer costs
//! the data plane nothing.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use flz_core::entry_input::{Episode, IdleWatch};

use crate::util::LockRecover;

/// Tick floor and ceiling. The floor bounds how often a fleet of observers
/// wakes; the ceiling bounds how late an episode is reported.
const TICK_MIN: Duration = Duration::from_millis(250);
const TICK_MAX: Duration = Duration::from_secs(1);

/// What an application does about an entry channel that has gone quiet.
///
/// Registered per channel with a threshold; the SDK reports **episodes**, not
/// ticks: [`idle`](Self::idle) fires once when the silence first exceeds the
/// threshold and not again while it continues, and
/// [`resumed`](Self::resumed) fires once when a frame finally arrives, re-arming
/// the next episode. Silence before the very first frame is never an episode —
/// an operative waiting for its subscriptions to take is not an incident.
///
/// Both calls run on the SDK's background tick. A panic is caught and logged;
/// delivery and later episodes are unaffected.
pub trait IdleObserver: Send + Sync + 'static {
    /// The channel has been silent for `gap`, which first exceeded the
    /// registered threshold.
    fn idle(&self, gap: Duration);

    /// A frame arrived, ending an episode that had lasted `gap`. Defaults to
    /// doing nothing — an alarm that only needs raising overrides `idle` alone.
    ///
    /// `gap` is measured to the newest frame the tick saw, so a burst that
    /// resumes and continues within one tick reports the silence plus that
    /// burst — bounded by the tick, i.e. at most a quarter of the threshold.
    /// The delivery path stores one stamp, not a first-frame-after-silence.
    fn resumed(&self, _gap: Duration) {}
}

impl<F: Fn(Duration) + Send + Sync + 'static> IdleObserver for F {
    fn idle(&self, gap: Duration) {
        self(gap)
    }
}

/// One registration: the observer, and the core's latch over the channel.
struct Watch {
    channel: Arc<str>,
    observer: Arc<dyn IdleObserver>,
    latch: IdleWatch,
}

/// One episode found by a tick, lifted out of the watch list so the observer
/// runs unlocked.
struct Pending {
    channel: Arc<str>,
    observer: Arc<dyn IdleObserver>,
    episode: Episode,
}

impl Pending {
    /// Invoke the observer, contained: application code runs on the SDK's own
    /// thread, so an unwind must become a log line rather than a dead ticker.
    fn deliver(self) {
        let call = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match self.episode {
            Episode::Idle(gap) => self.observer.idle(gap),
            Episode::Resumed(gap) => self.observer.resumed(gap),
        }));
        if let Err(panic) = call {
            tracing::warn!(
                target: "flezha::entry_idle",
                channel = %self.channel,
                episode = match self.episode {
                    Episode::Idle(_) => "idle",
                    Episode::Resumed(_) => "resumed",
                },
                "entry-idle observer panicked: {}",
                crate::util::format_panic(panic)
            );
        }
    }
}

/// One runner's idle observers and the single thread that evaluates them.
/// Owned by the runner core, so the ticker ends when the runner does.
pub(crate) struct IdleWatcher {
    watches: Mutex<Vec<Watch>>,
    /// Whether the tick thread has been spawned. The first registration
    /// spawns it; later ones just extend the list.
    ticking: AtomicBool,
}

impl IdleWatcher {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            watches: Mutex::new(Vec::new()),
            ticking: AtomicBool::new(false),
        })
    }

    /// Register `observer` against `channel`. Legal before and after
    /// `start()`: the observation cell is interned by name, so a registration
    /// made before the class is defined watches the same channel it later gets.
    pub(crate) fn register(
        self: &Arc<Self>,
        channel: String,
        threshold: Duration,
        observer: Arc<dyn IdleObserver>,
    ) {
        self.watches.lock_recover().push(Watch {
            latch: IdleWatch::new(&channel, threshold),
            channel: Arc::from(channel),
            observer,
        });
        if !self.ticking.swap(true, Ordering::AcqRel) {
            let watcher = Arc::downgrade(self);
            std::thread::Builder::new()
                .name("flezha-entry-idle".to_string())
                .spawn(move || run_ticks(watcher))
                .expect("spawning the flezha-entry-idle thread should not fail");
        }
    }

    /// Evaluate every watch once and deliver the episodes it found. Observers
    /// are called with the watch list unlocked so a slow one cannot block a
    /// concurrent registration.
    fn tick(&self) {
        let now = crate::util::monotonic_nanos();
        let pending: Vec<Pending> = self
            .watches
            .lock_recover()
            .iter_mut()
            .filter_map(|watch| {
                watch.latch.evaluate(now).map(|episode| Pending {
                    channel: Arc::clone(&watch.channel),
                    observer: Arc::clone(&watch.observer),
                    episode,
                })
            })
            .collect();
        for episode in pending {
            episode.deliver();
        }
    }

    /// How long to wait before the next pass: the shortest tick any registered
    /// threshold asks for.
    fn period(&self) -> Duration {
        self.watches
            .lock_recover()
            .iter()
            .map(|watch| tick_period(watch.latch.threshold()))
            .min()
            .unwrap_or(TICK_MAX)
    }
}

/// The tick a `threshold` asks for: quarter of it, so an episode is reported
/// within 25% of the threshold, bounded at both ends.
fn tick_period(threshold: Duration) -> Duration {
    (threshold / 4).clamp(TICK_MIN, TICK_MAX)
}

/// The tick loop. Holds only a `Weak`, so dropping the runner ends it.
fn run_ticks(watcher: Weak<IdleWatcher>) {
    while let Some(live) = watcher.upgrade() {
        live.tick();
        let period = live.period();
        drop(live);
        std::thread::sleep(period);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_tick_is_a_quarter_of_the_threshold_within_its_bounds() {
        assert_eq!(tick_period(Duration::from_secs(60)), TICK_MAX);
        assert_eq!(
            tick_period(Duration::from_secs(2)),
            Duration::from_millis(500)
        );
        assert_eq!(tick_period(Duration::from_millis(10)), TICK_MIN);
    }
}
