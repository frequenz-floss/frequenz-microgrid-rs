// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Representation of a pool of steam boilers in the microgrid.
//!
//! A [`SteamBoilerPool`] aggregates a set of steam boilers — either an explicit
//! subset or every steam boiler in the microgrid — and exposes their combined
//! active power, their aggregated active-power bounds, and a
//! health-partitioned telemetry snapshot stream.
//!
//! Obtain one from [`Microgrid::steam_boiler_pool`]; see [`SteamBoilerPool`]
//! for a usage example.
//!
//! [`Microgrid::steam_boiler_pool`]: crate::Microgrid::steam_boiler_pool

use tokio::sync::broadcast;

use std::collections::{BTreeSet, HashSet};
use std::time::Duration;

use crate::{
    Bounds, Error, Formula, LogicalMeterHandle, MicrogridClientHandle,
    client::proto::common::microgrid::electrical_components::{
        ElectricalComponentCategory, ElectricalComponentStateCode,
    },
    metric,
    metric::Metric,
    microgrid::{
        caching_sender::{CachingSender, WeakCachingSender},
        pool_bounds,
        pool_bounds_tracker::PoolBoundsTracker,
        pool_validation::validate_pool_ids,
        telemetry_tracker::steam_boiler_pool_telemetry_tracker::{
            SteamBoilerPoolSnapshot, SteamBoilerPoolTelemetryTracker,
        },
    },
    quantity::Power,
};

/// A pool of steam boilers in the microgrid.
///
/// Created with [`Microgrid::steam_boiler_pool`][mg], passing either an
/// explicit set of steam boiler component IDs or `None` to cover every steam
/// boiler in the microgrid. It exposes:
///
/// - [`power`](Self::power) — a [`Formula`] for the pool's aggregate active
///   power (passive sign convention: positive while the boilers consume);
/// - [`power_bounds`](Self::power_bounds) — a stream of the pool's aggregated
///   active-power bounds;
/// - [`telemetry_snapshots`](Self::telemetry_snapshots) — a stream of
///   [`SteamBoilerPoolSnapshot`]s partitioning the boilers into healthy and
///   unhealthy sets.
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
/// // A pool over every steam boiler in the microgrid.
/// let mut boilers = microgrid.steam_boiler_pool(None)?;
///
/// // Subscribe to the pool's aggregated active-power bounds.
/// let mut bounds_rx = boilers.power_bounds();
/// while let Ok(bounds) = bounds_rx.recv().await {
///     println!("Steam boiler pool active-power bounds: {bounds:?}");
/// }
/// # Ok(())
/// # }
/// ```
///
/// [mg]: crate::Microgrid::steam_boiler_pool
pub struct SteamBoilerPool {
    component_ids: Option<BTreeSet<u64>>,
    client: MicrogridClientHandle,
    logical_meter: LogicalMeterHandle,
    snapshot_tx: Option<WeakCachingSender<SteamBoilerPoolSnapshot>>,
    bounds_tx: Option<WeakCachingSender<Vec<Bounds<Power>>>>,
}

impl SteamBoilerPool {
    /// Creates a new `SteamBoilerPool` instance with the given component IDs,
    /// client and logical meter handles.
    ///
    /// When `component_ids` is `Some`, every ID must refer to a steam boiler in
    /// the component graph; otherwise an error is returned. When it is `None`,
    /// the pool covers all steam boilers in the microgrid.
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
            &this.get_all_steam_boiler_ids(),
            "steam boilers",
        )
        .inspect_err(|e| tracing::error!("{e}"))?;
        Ok(this)
    }

    fn get_all_steam_boiler_ids(&self) -> BTreeSet<u64> {
        self.logical_meter
            .graph()
            .components()
            .filter(|c| c.category() == ElectricalComponentCategory::SteamBoiler)
            .map(|c| c.id)
            .collect()
    }

    fn get_steam_boiler_ids(&self) -> BTreeSet<u64> {
        if let Some(ids) = &self.component_ids {
            ids.clone()
        } else {
            self.get_all_steam_boiler_ids()
        }
    }

    /// Returns a formula for the active power of the steam boiler pool.
    pub fn power(&mut self) -> Result<Formula<Power>, Error> {
        self.logical_meter
            .steam_boiler::<metric::AcPowerActive>(self.component_ids.clone())
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
            pool_bounds::compute_steam_boiler_pool_bounds::<metric::AcPowerActive>,
            format!("{} steam boiler", metric::AcPowerActive::str_name()),
        );
        tokio::spawn(tracker.run());
        self.bounds_tx = Some(tx.downgrade());
        rx
    }

    /// Returns a receiver for a stream of [`SteamBoilerPoolSnapshot`] values,
    /// each reflecting the latest boiler telemetry partitioned into healthy and
    /// unhealthy sets.
    ///
    /// Reuses the running tracker if one exists and still has active receivers
    /// (including any held by a bounds tracker); otherwise starts a new one.
    pub fn telemetry_snapshots(&mut self) -> broadcast::Receiver<SteamBoilerPoolSnapshot> {
        if let Some(tx) = self
            .snapshot_tx
            .as_ref()
            .and_then(WeakCachingSender::upgrade)
            && tx.receiver_count() > 0
        {
            return tx.subscribe_with_current();
        }
        let tx = CachingSender::<SteamBoilerPoolSnapshot>::new();
        // Subscribe before spawning so the tracker sees a receiver and doesn't
        // stop before this consumer has read anything.
        let rx = tx.subscribe_with_current();
        let tracker = SteamBoilerPoolTelemetryTracker::new(
            self.get_steam_boiler_ids(),
            Duration::from_secs(10),
            // Operational states in which a steam boiler is alive and
            // reporting usable telemetry: heating (Charging) or idle and
            // ready (Ready).
            HashSet::from([
                ElectricalComponentStateCode::Ready,
                ElectricalComponentStateCode::Charging,
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

    use super::SteamBoilerPool;
    use crate::Bounds;
    use crate::client::proto::common::microgrid::electrical_components::ElectricalComponentStateCode;
    use crate::client::test_utils::MockComponent;
    use crate::microgrid::test_utils::{handles, last_snapshot};
    use crate::quantity::Power;

    /// grid → meter → [boiler meter → steam_boiler(4), steam_boiler(5)],
    ///                 [chp meter → chp(7)]
    fn graph() -> MockComponent {
        MockComponent::grid(1).with_children(vec![MockComponent::meter(2).with_children(vec![
            MockComponent::meter(3).with_children(vec![
                MockComponent::steam_boiler(4),
                MockComponent::steam_boiler(5),
            ]),
            MockComponent::meter(6).with_children(vec![MockComponent::chp(7)]),
        ])])
    }

    #[tokio::test]
    async fn try_new_accepts_empty_component_ids() {
        let (client, lm) = handles(graph()).await;
        let mut pool = SteamBoilerPool::try_new(Some(BTreeSet::new()), client, lm)
            .expect("an empty component_ids set should yield an empty pool");
        pool.power().expect("empty pool power formula");
    }

    #[tokio::test]
    async fn try_new_rejects_non_steam_boiler_component_ids() {
        let (client, lm) = handles(graph()).await;
        // 7 is a CHP: a valid component, but not a steam boiler.
        let err = SteamBoilerPool::try_new(Some([7].into()), client, lm)
            .err()
            .expect("a CHP ID should be rejected");
        assert!(
            err.to_string().contains("must be steam boilers"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn try_new_rejects_unknown_component_ids() {
        let (client, lm) = handles(graph()).await;
        // 99 is not in the graph at all.
        let err = SteamBoilerPool::try_new(Some([4, 99].into()), client, lm)
            .err()
            .expect("an unknown ID should be rejected");
        assert!(
            err.to_string().contains("must be steam boilers"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn power_formula_for_all_steam_boilers() {
        let (client, lm) = handles(graph()).await;
        let mut pool = SteamBoilerPool::try_new(None, client, lm).unwrap();
        assert_eq!(
            pool.power().unwrap().to_string(),
            "METRIC_AC_POWER_ACTIVE::(COALESCE(#5 + #4, #3, COALESCE(#5, 0.0) + COALESCE(#4, 0.0)))"
        );
    }

    #[tokio::test]
    async fn power_formula_for_explicit_steam_boiler() {
        let (client, lm) = handles(graph()).await;
        let mut pool = SteamBoilerPool::try_new(Some([4].into()), client, lm).unwrap();
        assert_eq!(
            pool.power().unwrap().to_string(),
            "METRIC_AC_POWER_ACTIVE::(COALESCE(#4, #3 - #5, 0.0))"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn empty_pool_emits_empty_snapshot_and_bounds() {
        // grid → meter, with no steam boilers anywhere.
        let (client, lm) =
            handles(MockComponent::grid(1).with_children(vec![MockComponent::meter(2)])).await;
        let mut pool = SteamBoilerPool::try_new(None, client, lm).unwrap();

        let mut snapshots = pool.telemetry_snapshots();
        let mut bounds = pool.power_bounds();

        let snapshot = last_snapshot(&mut snapshots, 5).await;
        assert!(
            snapshot.boilers.healthy.is_empty() && snapshot.boilers.unhealthy.is_empty(),
            "empty pool snapshot should have no boilers, got {snapshot:?}"
        );
        assert!(
            last_snapshot(&mut bounds, 5).await.is_empty(),
            "empty pool should have empty power bounds"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn reporting_boilers_are_healthy_and_their_bounds_add() {
        // grid → meter → [steam_boiler(3), steam_boiler(4)], both reporting
        // power with bounds 0..1000 W and 0..2000 W.
        let (client, lm) = handles(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                MockComponent::steam_boiler(3)
                    .with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
                    .add_power_bounds(Some(0.0), Some(1000.0)),
                MockComponent::steam_boiler(4)
                    .with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
                    .add_power_bounds(Some(0.0), Some(2000.0)),
            ]),
        ]))
        .await;
        let mut pool = SteamBoilerPool::try_new(None, client, lm).unwrap();

        let mut snapshots = pool.telemetry_snapshots();
        let mut bounds = pool.power_bounds();

        let snap = last_snapshot(&mut snapshots, 10).await;
        assert!(snap.boilers.healthy.contains_key(&3));
        assert!(snap.boilers.healthy.contains_key(&4));
        assert!(snap.boilers.unhealthy.is_empty());

        assert_eq!(
            last_snapshot(&mut bounds, 5).await,
            vec![Bounds::new(
                Some(Power::from_watts(0.0)),
                Some(Power::from_watts(3000.0))
            )]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn boiler_with_bad_state_is_unhealthy() {
        // Standby is healthy for a PV inverter but not for a steam boiler.
        let (client, lm) = handles(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                MockComponent::steam_boiler(3)
                    .with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
                    .with_state(ElectricalComponentStateCode::Standby),
            ]),
        ]))
        .await;
        let mut pool = SteamBoilerPool::try_new(None, client, lm).unwrap();

        let mut rx = pool.telemetry_snapshots();
        let snap = last_snapshot(&mut rx, 10).await;

        assert!(snap.boilers.healthy.is_empty());
        assert!(snap.boilers.unhealthy.contains_key(&3), "got {snap:?}");
        // The Standby sample was received and rejected, not just never seen.
        assert!(
            snap.boilers.unhealthy[&3].is_some(),
            "bad-state sample should be stored, got {snap:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn charging_boiler_is_healthy() {
        // Charging is the boiler's "heating" state and counts as healthy.
        let (client, lm) = handles(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                MockComponent::steam_boiler(3)
                    .with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
                    .with_state(ElectricalComponentStateCode::Charging),
            ]),
        ]))
        .await;
        let mut pool = SteamBoilerPool::try_new(None, client, lm).unwrap();

        let mut rx = pool.telemetry_snapshots();
        let snap = last_snapshot(&mut rx, 10).await;

        assert!(snap.boilers.healthy.contains_key(&3), "got {snap:?}");
        assert!(snap.boilers.unhealthy.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn calling_power_bounds_twice_reuses_the_tracker() {
        let (client, lm) = handles(graph()).await;
        let mut pool = SteamBoilerPool::try_new(None, client, lm).unwrap();

        let mut rx1 = pool.power_bounds();
        let bounds1 = last_snapshot(&mut rx1, 10).await;

        // A second call while rx1 is alive must reuse the running tracker, which
        // re-sends its cached bounds at once; a fresh tracker's cache would be
        // empty until it ran.
        let mut rx2 = pool.power_bounds();
        let bounds2 = rx2
            .try_recv()
            .expect("reused tracker should re-send its cached bounds immediately");
        assert_eq!(bounds1, bounds2);
    }
}
