// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Representation of a pool of batteries in the microgrid.

use tokio::sync::broadcast;

use std::collections::{BTreeSet, HashSet};
use std::time::Duration;

use crate::{
    Bounds, Error, Formula, LogicalMeterHandle, MicrogridClientHandle,
    client::{
        ElectricalComponentCategory,
        proto::common::microgrid::electrical_components::ElectricalComponentStateCode,
    },
    metric,
    metric::Metric,
    microgrid::{
        battery_bounds_tracker,
        pool_bounds_tracker::PoolBoundsTracker,
        pool_broadcast::try_reuse,
        telemetry_tracker::battery_pool_telemetry_tracker::{
            BatteryPoolSnapshot, BatteryPoolTelemetryTracker,
        },
    },
    quantity::Power,
};

/// An interface for abstracting over a pool of batteries in the microgrid.
pub struct BatteryPool {
    component_ids: Option<BTreeSet<u64>>,
    client: MicrogridClientHandle,
    logical_meter: LogicalMeterHandle,
    snapshot_tx: Option<broadcast::WeakSender<BatteryPoolSnapshot>>,
    bounds_tx: Option<broadcast::WeakSender<Vec<Bounds<Power>>>>,
}

impl BatteryPool {
    /// Creates a new `BatteryPool` instance with the given component IDs,
    /// client and logical meter handles.
    pub(crate) fn try_new(
        component_ids: Option<BTreeSet<u64>>,
        client: MicrogridClientHandle,
        logical_meter: LogicalMeterHandle,
    ) -> Result<Self, Error> {
        let this = Self {
            component_ids,
            client,
            logical_meter,
            snapshot_tx: None,
            bounds_tx: None,
        };
        if let Some(ids) = &this.component_ids {
            if ids.is_empty() {
                let e = "component_ids cannot be an empty set".to_string();
                tracing::error!("{e}");
                return Err(Error::invalid_component(e));
            }
            // Validate that all provided IDs correspond to batteries in the graph.
            if !ids.is_subset(&this.get_all_battery_ids()) {
                let e = format!("All component_ids {:?} must be batteries.", ids);
                tracing::error!("{e}");
                return Err(Error::invalid_component(e));
            }
        }
        Ok(this)
    }

    fn get_all_battery_ids(&self) -> BTreeSet<u64> {
        self.logical_meter
            .graph()
            .components()
            .filter(|c| c.category() == ElectricalComponentCategory::Battery)
            .map(|c| c.id)
            .collect()
    }

    pub(crate) fn get_battery_ids(&self) -> BTreeSet<u64> {
        if let Some(ids) = &self.component_ids {
            ids.clone()
        } else {
            self.get_all_battery_ids()
        }
    }

    /// Returns a formula for the active power of the battery pool.
    pub fn power(&mut self) -> Result<Formula<Power>, Error> {
        self.logical_meter
            .battery::<metric::AcPowerActive>(self.component_ids.clone())
    }

    /// Returns a receiver for the aggregated active-power bounds of the pool,
    /// updated on each snapshot.
    ///
    /// Reuses the running bounds tracker if one exists and still has active
    /// receivers; otherwise starts a new one (which also starts or reuses the
    /// underlying telemetry tracker).
    pub fn power_bounds(&mut self) -> broadcast::Receiver<Vec<Bounds<Power>>> {
        if let Some(rx) = try_reuse(&self.bounds_tx) {
            return rx;
        }
        let snapshot_rx = self.telemetry_snapshots();
        let (tx, rx) = broadcast::channel(100);
        self.bounds_tx = Some(tx.downgrade());
        let tracker = PoolBoundsTracker::new(
            snapshot_rx,
            tx,
            battery_bounds_tracker::compute_pool_bounds::<metric::AcPowerActive, metric::DcPower>,
            format!(
                "{}/{}",
                metric::AcPowerActive::str_name(),
                metric::DcPower::str_name()
            ),
        );
        tokio::spawn(tracker.run());
        rx
    }

    /// Returns a receiver for a stream of [`BatteryPoolSnapshot`] values,
    /// each reflecting the latest component telemetry partitioned into
    /// healthy and unhealthy sets.
    ///
    /// Reuses the running tracker if one exists and still has active receivers
    /// (including any held by a bounds tracker); otherwise starts a new one.
    pub fn telemetry_snapshots(&mut self) -> broadcast::Receiver<BatteryPoolSnapshot> {
        if let Some(rx) = try_reuse(&self.snapshot_tx) {
            return rx;
        }
        let (tx, rx) = broadcast::channel(100);
        self.snapshot_tx = Some(tx.downgrade());
        let tracker = BatteryPoolTelemetryTracker::new(
            self.get_battery_ids(),
            Duration::from_secs(10),
            HashSet::from([
                ElectricalComponentStateCode::Ready,
                ElectricalComponentStateCode::Standby,
                ElectricalComponentStateCode::Charging,
                ElectricalComponentStateCode::Discharging,
                ElectricalComponentStateCode::RelayClosed,
            ]),
            self.client.clone(),
            self.logical_meter.clone(),
            tx,
        );
        tokio::spawn(tracker.run());
        rx
    }
}
