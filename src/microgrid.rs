// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! High-level interface for the Microgrid API.

mod battery_pool;
pub use battery_pool::BatteryPool;

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
    /// Returns an error if the URL is unreachable, or if the component graph
    /// cannot be created with the given configuration.
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

    pub fn battery_pool(&self, component_ids: Option<Vec<u64>>) -> BatteryPool {
        BatteryPool::new(
            component_ids.map(|ids| ids.into_iter().collect()),
            self.client.clone(),
            self.logical_meter.clone(),
        )
    }
}
