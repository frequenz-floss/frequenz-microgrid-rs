// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Representation of a pool of PV inverters in the microgrid.
//!
//! A [`PvPool`] aggregates a set of PV inverters — either an explicit subset or
//! every PV inverter in the microgrid — and exposes their combined active
//! power, their aggregated active-power bounds, and a health-partitioned
//! telemetry snapshot stream.
//!
//! Obtain one from [`Microgrid::pv_pool`]; see [`PvPool`] for a usage example.
//!
//! [`Microgrid::pv_pool`]: crate::Microgrid::pv_pool

use tokio::sync::broadcast;

use std::collections::{BTreeSet, HashSet};
use std::time::Duration;

use crate::{
    Bounds, Error, Formula, LogicalMeterHandle, MicrogridClientHandle,
    client::proto::common::microgrid::electrical_components::ElectricalComponentStateCode,
    metric,
    microgrid::{
        pv_bounds_tracker::PvPoolBoundsTracker,
        telemetry_tracker::pv_pool_telemetry_tracker::{PvPoolSnapshot, PvPoolTelemetryTracker},
    },
    quantity::Power,
};

/// A pool of PV inverters in the microgrid.
///
/// Created with [`Microgrid::pv_pool`][mg], passing either an explicit set of PV
/// inverter component IDs or `None` to cover every PV inverter in the microgrid.
/// It exposes:
///
/// - [`power`](Self::power) — a [`Formula`] for the pool's aggregate active
///   power;
/// - [`power_bounds`](Self::power_bounds) — a stream of the pool's aggregated
///   active-power bounds;
/// - [`telemetry_snapshots`](Self::telemetry_snapshots) — a stream of
///   [`PvPoolSnapshot`]s partitioning the inverters into healthy and unhealthy
///   sets.
///
/// The bounds and snapshot streams share a telemetry tracker that is started on
/// first use and reused while it still has live receivers.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), frequenz_microgrid::Error> {
/// use chrono::TimeDelta;
/// use frequenz_microgrid::{LogicalMeterConfig, Microgrid};
///
/// let microgrid = Microgrid::try_new(
///     "grpc://localhost:50051",
///     LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap()),
/// )
/// .await?;
///
/// // A pool over every PV inverter in the microgrid.
/// let mut pv_pool = microgrid.pv_pool(None)?;
///
/// // Subscribe to the pool's aggregated active-power bounds.
/// let mut bounds_rx = pv_pool.power_bounds();
/// while let Ok(bounds) = bounds_rx.recv().await {
///     println!("PV pool active-power bounds: {bounds:?}");
/// }
/// # Ok(())
/// # }
/// ```
///
/// [mg]: crate::Microgrid::pv_pool
pub struct PvPool {
    component_ids: Option<BTreeSet<u64>>,
    client: MicrogridClientHandle,
    logical_meter: LogicalMeterHandle,
    snapshot_tx: Option<broadcast::WeakSender<PvPoolSnapshot>>,
    bounds_tx: Option<broadcast::WeakSender<Vec<Bounds<Power>>>>,
}

impl PvPool {
    /// Creates a new `PvPool` instance with the given component IDs, client and
    /// logical meter handles.
    ///
    /// When `component_ids` is `Some`, every ID must refer to a PV inverter in
    /// the component graph; otherwise an error is returned. When it is `None`,
    /// the pool covers all PV inverters in the microgrid.
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
            // Validate that all provided IDs correspond to PV inverters in the
            // graph.
            if !ids.is_subset(&this.get_all_pv_inverter_ids()) {
                let e = format!("All component_ids {:?} must be PV inverters.", ids);
                tracing::error!("{e}");
                return Err(Error::invalid_component(e));
            }
        }
        Ok(this)
    }

    fn get_all_pv_inverter_ids(&self) -> BTreeSet<u64> {
        self.logical_meter
            .graph()
            .components()
            .filter(|c| c.is_pv_inverter())
            .map(|c| c.id)
            .collect()
    }

    pub(crate) fn get_pv_inverter_ids(&self) -> BTreeSet<u64> {
        if let Some(ids) = &self.component_ids {
            ids.clone()
        } else {
            self.get_all_pv_inverter_ids()
        }
    }

    /// Returns a formula for the active power of the PV pool.
    pub fn power(&mut self) -> Result<Formula<Power>, Error> {
        self.logical_meter
            .pv::<metric::AcPowerActive>(self.component_ids.clone())
    }

    /// Returns a receiver for the aggregated active-power bounds of the pool,
    /// updated on each snapshot.
    ///
    /// Reuses the running bounds tracker if one exists and still has active
    /// receivers; otherwise starts a new one (which also starts or reuses the
    /// underlying telemetry tracker).
    pub fn power_bounds(&mut self) -> broadcast::Receiver<Vec<Bounds<Power>>> {
        if let Some(tx) = self
            .bounds_tx
            .as_ref()
            .and_then(broadcast::WeakSender::upgrade)
            && tx.receiver_count() > 0
        {
            return tx.subscribe();
        }
        let snapshot_rx = self.telemetry_snapshots();
        let (tx, rx) = broadcast::channel(100);
        self.bounds_tx = Some(tx.downgrade());
        let tracker = PvPoolBoundsTracker::<metric::AcPowerActive>::new(snapshot_rx, tx);
        tokio::spawn(tracker.run());
        rx
    }

    /// Returns a receiver for a stream of [`PvPoolSnapshot`] values, each
    /// reflecting the latest inverter telemetry partitioned into healthy and
    /// unhealthy sets.
    ///
    /// Reuses the running tracker if one exists and still has active receivers
    /// (including any held by a bounds tracker); otherwise starts a new one.
    pub fn telemetry_snapshots(&mut self) -> broadcast::Receiver<PvPoolSnapshot> {
        if let Some(tx) = self
            .snapshot_tx
            .as_ref()
            .and_then(broadcast::WeakSender::upgrade)
            && tx.receiver_count() > 0
        {
            return tx.subscribe();
        }
        let (tx, rx) = broadcast::channel(100);
        self.snapshot_tx = Some(tx.downgrade());
        let tracker = PvPoolTelemetryTracker::new(
            self.get_pv_inverter_ids(),
            Duration::from_secs(10),
            // Operational states in which a PV inverter is alive and
            // reporting usable telemetry: producing (Discharging), or idle
            // and ready (Ready / Standby).
            HashSet::from([
                ElectricalComponentStateCode::Ready,
                ElectricalComponentStateCode::Standby,
                ElectricalComponentStateCode::Discharging,
            ]),
            self.client.clone(),
            tx,
        );
        tokio::spawn(tracker.run());
        rx
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::TimeDelta;

    use super::PvPool;
    use crate::{
        LogicalMeterConfig, LogicalMeterHandle, MicrogridClientHandle,
        client::test_utils::{MockComponent, MockMicrogridApiClient},
    };

    /// Builds client and logical-meter handles backed by the given mock graph.
    async fn handles(graph: MockComponent) -> (MicrogridClientHandle, LogicalMeterHandle) {
        let api = MockMicrogridApiClient::new(graph);
        let client = MicrogridClientHandle::new_from_client(api);
        let lm = LogicalMeterHandle::try_new(
            client.clone(),
            LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap()),
        )
        .await
        .unwrap();
        (client, lm)
    }

    /// grid → meter → [pv meter → pv_inverter(4), pv_inverter(5)],
    ///                 [battery meter → battery_inverter(7) → battery(8)]
    fn graph() -> MockComponent {
        MockComponent::grid(1).with_children(vec![MockComponent::meter(2).with_children(vec![
            MockComponent::meter(3).with_children(vec![
                MockComponent::pv_inverter(4),
                MockComponent::pv_inverter(5),
            ]),
            MockComponent::meter(6).with_children(vec![
                MockComponent::battery_inverter(7).with_children(vec![MockComponent::battery(8)]),
            ]),
        ])])
    }

    #[tokio::test]
    async fn try_new_rejects_empty_component_ids() {
        let (client, lm) = handles(graph()).await;
        let err = PvPool::try_new(Some(BTreeSet::new()), client, lm)
            .err()
            .expect("empty component_ids should be rejected");
        assert!(err.to_string().contains("empty"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn try_new_rejects_non_pv_component_ids() {
        let (client, lm) = handles(graph()).await;
        // 7 is a battery inverter and 8 a battery — neither is a PV inverter.
        let err = PvPool::try_new(Some([4, 7, 8].into()), client, lm)
            .err()
            .expect("non-PV component_ids should be rejected");
        assert!(
            err.to_string().contains("must be PV inverters"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn power_formula_for_explicit_pv_inverters() {
        let (client, lm) = handles(graph()).await;
        let mut pool = PvPool::try_new(Some([4, 5].into()), client, lm).unwrap();
        let formula = pool.power().unwrap();
        assert_eq!(
            formula.to_string(),
            "METRIC_AC_POWER_ACTIVE::(COALESCE(#5 + #4, #3, COALESCE(#5, 0.0) + COALESCE(#4, 0.0)))"
        );
    }

    #[tokio::test]
    async fn power_formula_for_all_pv_inverters() {
        let (client, lm) = handles(graph()).await;
        let mut pool = PvPool::try_new(None, client, lm).unwrap();
        let formula = pool.power().unwrap();
        assert_eq!(
            formula.to_string(),
            "METRIC_AC_POWER_ACTIVE::(COALESCE(#5 + #4, #3, COALESCE(#5, 0.0) + COALESCE(#4, 0.0)))"
        );
    }
}
