// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Representation of a pool of batteries in the microgrid.

use std::collections::BTreeSet;

use crate::{Error, Formula, LogicalMeterHandle, MicrogridClientHandle, metric, quantity::Power};

/// An interface for abstracting over a pool of batteries in the microgrid.
pub struct BatteryPool {
    component_ids: Option<BTreeSet<u64>>,
    client: MicrogridClientHandle,
    logical_meter: LogicalMeterHandle,
}

impl BatteryPool {
    /// Creates a new `BatteryPool` instance with the given component IDs,
    /// client and logical meter handles.
    pub(crate) fn new(
        component_ids: Option<BTreeSet<u64>>,
        client: MicrogridClientHandle,
        logical_meter: LogicalMeterHandle,
    ) -> Self {
        Self {
            component_ids,
            client,
            logical_meter,
        }
    }

    /// Returns a formula for the active power of the battery pool.
    pub fn power(&mut self) -> Result<Formula<Power>, Error> {
        self.logical_meter
            .battery::<metric::AcPowerActive>(self.component_ids.clone())
    }
}
