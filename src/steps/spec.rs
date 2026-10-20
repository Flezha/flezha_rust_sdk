//! The typed half of steps-spec construction: `build_spec::<T>()` runs the
//! author's `channels` through the shared check and adds the class's steps
//! facts. The spec/DAG data structures live in `flz_core::steps::spec`.

use std::sync::Arc;

use flz_core::relay::topology::RelayTopology;
pub use flz_core::steps::spec::*;

use crate::relay::builder::{build_topology_for, TopologyBuildError};
use crate::relay::Relay;

/// Build the spec for a class that runs in steps: its `channels()`
/// declaration, judged by the core's check, with its DAG and settings.
pub fn build_spec<T: Relay>() -> Result<Arc<RelayTopology>, TopologyBuildError> {
    let mut topology = Arc::into_inner(build_topology_for::<T>()?)
        .expect("a topology just built is shared with nobody");
    topology.steps = StepsSpec::of(&topology, &T::settings().run);
    Ok(Arc::new(topology))
}
