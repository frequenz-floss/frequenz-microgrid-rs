// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! A set of components partitioned by health status.

use std::collections::HashMap;

use crate::client::proto::common::microgrid::electrical_components::ElectricalComponentTelemetry;

/// A set of components partitioned by health status and annotated with the
/// latest telemetry sample for each.
///
/// `healthy` holds the most recent [`ElectricalComponentTelemetry`] observed
/// for each healthy component. `unhealthy` holds the last telemetry observed
/// before the component became unhealthy, or `None` if no sample has been
/// received yet. Consumers can use the telemetry (including per-metric bounds)
/// directly without subscribing to the raw streams again.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ComponentHealthPartition {
    pub healthy: HashMap<u64, ElectricalComponentTelemetry>,
    pub unhealthy: HashMap<u64, Option<ElectricalComponentTelemetry>>,
}

impl ComponentHealthPartition {
    /// Records `data` as the latest telemetry for the now-healthy component
    /// `id`, removing it from the unhealthy set.
    pub(crate) fn mark_healthy(&mut self, id: u64, data: ElectricalComponentTelemetry) {
        self.healthy.insert(id, data);
        self.unhealthy.remove(&id);
    }

    /// Records component `id` as unhealthy, carrying its last telemetry sample
    /// if any, and removing it from the healthy set.
    pub(crate) fn mark_unhealthy(&mut self, id: u64, data: Option<ElectricalComponentTelemetry>) {
        self.unhealthy.insert(id, data);
        self.healthy.remove(&id);
    }
}
