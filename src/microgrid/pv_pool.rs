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
    metric::Metric,
    microgrid::{
        caching_sender::{CachingSender, WeakCachingSender},
        pool_bounds,
        pool_bounds_tracker::PoolBoundsTracker,
        pool_validation::validate_pool_ids,
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
    snapshot_tx: Option<WeakCachingSender<PvPoolSnapshot>>,
    bounds_tx: Option<WeakCachingSender<Vec<Bounds<Power>>>>,
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
        validate_pool_ids(
            &this.component_ids,
            &this.get_all_pv_inverter_ids(),
            "PV inverters",
        )
        .inspect_err(|e| tracing::error!("{e}"))?;
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
        if let Some(tx) = self.bounds_tx.as_ref().and_then(WeakCachingSender::upgrade)
            && tx.receiver_count() > 0
        {
            return tx.subscribe_with_current();
        }
        let snapshot_rx = self.telemetry_snapshots();
        let tx = CachingSender::<Vec<Bounds<Power>>>::new();
        // Subscribe before spawning so the tracker sees a receiver and doesn't
        // stop before this consumer has read anything.
        let rx = tx.subscribe_with_current();
        let tracker = PoolBoundsTracker::new(
            snapshot_rx,
            tx.clone(),
            pool_bounds::compute_pv_pool_bounds::<metric::AcPowerActive>,
            format!("{} PV", metric::AcPowerActive::str_name()),
        );
        tokio::spawn(tracker.run());
        self.bounds_tx = Some(tx.downgrade());
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
            .and_then(WeakCachingSender::upgrade)
            && tx.receiver_count() > 0
        {
            return tx.subscribe_with_current();
        }
        let tx = CachingSender::<PvPoolSnapshot>::new();
        // Subscribe before spawning so the tracker sees a receiver and doesn't
        // stop before this consumer has read anything.
        let rx = tx.subscribe_with_current();
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
            tx.clone(),
        );
        tokio::spawn(tracker.run());
        self.snapshot_tx = Some(tx.downgrade());
        rx
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::PvPool;
    use crate::client::test_utils::MockComponent;
    use crate::microgrid::test_utils::{handles, last_snapshot};

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
    async fn try_new_accepts_empty_component_ids() {
        let (client, lm) = handles(graph()).await;
        // An explicit empty selection is a valid (empty) pool, not an error.
        let mut pool = PvPool::try_new(Some(BTreeSet::new()), client, lm)
            .expect("an empty component_ids set should yield an empty pool");
        pool.power().expect("empty pool power formula");
    }

    #[tokio::test(start_paused = true)]
    async fn empty_pool_emits_empty_snapshot_and_bounds() {
        // grid → meter, with no PV inverters anywhere.
        let (client, lm) =
            handles(MockComponent::grid(1).with_children(vec![MockComponent::meter(2)])).await;
        let mut pool = PvPool::try_new(None, client, lm).unwrap();

        let mut snapshots = pool.telemetry_snapshots();
        let mut bounds = pool.power_bounds();

        let snapshot = last_snapshot(&mut snapshots, 5).await;
        assert!(
            snapshot.inverters.healthy.is_empty() && snapshot.inverters.unhealthy.is_empty(),
            "empty pool snapshot should have no inverters, got {snapshot:?}"
        );

        let bounds = last_snapshot(&mut bounds, 5).await;
        assert!(
            bounds.is_empty(),
            "empty pool should have empty power bounds"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn late_subscriber_sees_latest_snapshot() {
        let (client, lm) = handles(graph()).await;
        let mut pool = PvPool::try_new(None, client, lm).unwrap();

        // Drive the tracker so it has published a non-initial snapshot (the two
        // PV inverters carry no telemetry, so they settle into the unhealthy
        // set).
        let mut early = pool.telemetry_snapshots();
        let early_snap = last_snapshot(&mut early, 10).await;
        assert_eq!(
            early_snap.inverters.unhealthy.len(),
            2,
            "precondition: both inverters tracked"
        );

        // A subscriber joining after that publish must immediately observe the
        // current snapshot — `subscribe` re-sends the cached value, so it neither
        // blocks waiting for a change nor sees an empty stream.
        let mut late = pool.telemetry_snapshots();
        let late_snap = late
            .try_recv()
            .expect("late subscriber should be sent the cached snapshot at once");
        assert_eq!(
            late_snap, early_snap,
            "late subscriber should see the latest snapshot, not an empty stream"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn resubscribing_after_teardown_yields_a_current_valued_stream() {
        let (client, lm) = handles(graph()).await;
        let mut pool = PvPool::try_new(None, client, lm).unwrap();

        // Subscribe, drive to a real snapshot, then drop the only consumer so
        // the tracker stops (its next tick finds no receivers).
        let mut rx = pool.telemetry_snapshots();
        assert_eq!(
            last_snapshot(&mut rx, 10).await.inverters.unhealthy.len(),
            2
        );
        drop(rx);
        tokio::time::advance(std::time::Duration::from_secs(1)).await;

        // Resubscribe: with the previous tracker stopped, the pool starts a fresh
        // one, so the stream is immediately usable again — delivering the pool's
        // current snapshot instead of hanging.
        let mut rx = pool.telemetry_snapshots();
        assert_eq!(
            last_snapshot(&mut rx, 10).await.inverters.unhealthy.len(),
            2,
            "resubscribed stream should yield the pool's current snapshot"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn calling_power_bounds_twice_reuses_the_tracker() {
        let (client, lm) = handles(graph()).await;
        let mut pool = PvPool::try_new(None, client, lm).unwrap();

        // First call starts the bounds tracker; drive it so it caches a value.
        let mut rx1 = pool.power_bounds();
        let bounds1 = last_snapshot(&mut rx1, 10).await;

        // A second call while rx1 is still alive must reuse the running tracker
        // (its weak sender upgrades and still has a receiver). Reuse re-sends the
        // cached bounds at once; a freshly spawned tracker's cache would be empty
        // until it ran, so an immediate `try_recv` succeeds only on the reuse path.
        let mut rx2 = pool.power_bounds();
        let bounds2 = rx2
            .try_recv()
            .expect("reused tracker should re-send its cached bounds immediately");
        assert_eq!(bounds1, bounds2, "reused tracker shares the same bounds");
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
            concat!(
                "COALESCE(#5:AC_POWER_ACTIVE + #4:AC_POWER_ACTIVE, #3:AC_POWER_ACTIVE, ",
                "COALESCE(#5:AC_POWER_ACTIVE, 0) + COALESCE(#4:AC_POWER_ACTIVE, 0))"
            )
        );
    }

    #[tokio::test]
    async fn power_formula_for_all_pv_inverters() {
        let (client, lm) = handles(graph()).await;
        let mut pool = PvPool::try_new(None, client, lm).unwrap();
        let formula = pool.power().unwrap();
        assert_eq!(
            formula.to_string(),
            concat!(
                "COALESCE(#5:AC_POWER_ACTIVE + #4:AC_POWER_ACTIVE, #3:AC_POWER_ACTIVE, ",
                "COALESCE(#5:AC_POWER_ACTIVE, 0) + COALESCE(#4:AC_POWER_ACTIVE, 0))"
            )
        );
    }
}
