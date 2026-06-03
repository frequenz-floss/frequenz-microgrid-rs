// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! High-level interface for the Microgrid API.

mod battery_bounds_tracker;
mod battery_pool;
pub use battery_pool::BatteryPool;

mod pv_pool;
pub use pv_pool::PvPool;

pub(crate) mod telemetry_tracker;

use crate::{Error, LogicalMeterConfig, LogicalMeterHandle, MicrogridClientHandle};

/// A high-level interface for the Microgrid API.
pub struct Microgrid {
    client: MicrogridClientHandle,
    logical_meter: LogicalMeterHandle,
}

impl Microgrid {
    /// Creates a new `Microgrid` instance with the given microgrid API URL and
    /// logical meter configuration.
    ///
    /// The microgrid API connection is established lazily and connection or
    /// component-graph build errors during setup are retried indefinitely, so
    /// this call blocks until the server is reachable and returns valid data.
    /// Returns an error only if the URL is malformed or if the provided
    /// logical meter configuration is invalid.
    pub async fn try_new(
        url: impl Into<String>,
        config: LogicalMeterConfig,
    ) -> Result<Self, Error> {
        let client = MicrogridClientHandle::try_new(url).await?;
        let logical_meter = LogicalMeterHandle::try_new(client.clone(), config).await?;

        Ok(Microgrid {
            client,
            logical_meter,
        })
    }

    /// Creates a new `Microgrid` instance from the given client and logical
    /// meter handles.
    pub fn new_from_handles(
        client: MicrogridClientHandle,
        logical_meter: LogicalMeterHandle,
    ) -> Self {
        Microgrid {
            client,
            logical_meter,
        }
    }

    /// Returns a handle to the Microgrid client.
    pub fn client(&self) -> MicrogridClientHandle {
        self.client.clone()
    }

    /// Returns a handle to the logical meter.
    pub fn logical_meter(&self) -> LogicalMeterHandle {
        self.logical_meter.clone()
    }

    pub fn battery_pool(&self, component_ids: Option<Vec<u64>>) -> Result<BatteryPool, Error> {
        BatteryPool::try_new(
            component_ids.map(|ids| ids.into_iter().collect()),
            self.client.clone(),
            self.logical_meter.clone(),
        )
    }

    pub fn pv_pool(&self, component_ids: Option<Vec<u64>>) -> Result<PvPool, Error> {
        PvPool::try_new(
            component_ids.map(|ids| ids.into_iter().collect()),
            self.client.clone(),
            self.logical_meter.clone(),
        )
    }
}
