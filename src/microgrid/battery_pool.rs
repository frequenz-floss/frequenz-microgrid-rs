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
        battery_pool_formulas,
        caching_sender::{CachingSender, WeakCachingSender},
        pool_bounds,
        pool_bounds_tracker::PoolBoundsTracker,
        pool_validation::validate_pool_ids,
        telemetry_tracker::battery_pool_telemetry_tracker::{
            BatteryPoolSnapshot, BatteryPoolTelemetryTracker, InverterBatteryGroup,
        },
    },
    quantity::{Energy, Percentage, Power},
};

/// An interface for abstracting over a pool of batteries in the microgrid.
pub struct BatteryPool {
    component_ids: Option<BTreeSet<u64>>,
    client: MicrogridClientHandle,
    logical_meter: LogicalMeterHandle,
    /// The pool's batteries grouped with the inverters they sit behind.
    groups: Vec<InverterBatteryGroup>,
    snapshot_tx: Option<WeakCachingSender<BatteryPoolSnapshot>>,
    bounds_tx: Option<WeakCachingSender<Vec<Bounds<Power>>>>,
}

impl BatteryPool {
    /// Creates a new `BatteryPool` instance with the given component IDs,
    /// client and logical meter handles.
    pub(crate) fn try_new(
        component_ids: Option<BTreeSet<u64>>,
        client: MicrogridClientHandle,
        logical_meter: LogicalMeterHandle,
    ) -> Result<Self, Error> {
        let all_battery_ids = Self::battery_ids_in(&logical_meter);
        validate_pool_ids(&component_ids, &all_battery_ids, "batteries")
            .inspect_err(|e| tracing::error!("{e}"))?;
        let battery_ids = component_ids.clone().unwrap_or(all_battery_ids);
        // A malformed or partial selection (e.g. only one battery of an
        // inverter-battery group) is rejected here, before any tracker
        // is spawned. Errors are logged inside `inverter_battery_groups`.
        let groups = BatteryPoolTelemetryTracker::inverter_battery_groups(
            logical_meter.graph(),
            &battery_ids,
        )?;
        Ok(Self {
            component_ids,
            client,
            logical_meter,
            groups,
            snapshot_tx: None,
            bounds_tx: None,
        })
    }

    fn battery_ids_in(logical_meter: &LogicalMeterHandle) -> BTreeSet<u64> {
        logical_meter
            .graph()
            .components()
            .filter(|c| c.category() == ElectricalComponentCategory::Battery)
            .map(|c| c.id)
            .collect()
    }

    /// The ids of the pool's batteries: the union of its groups' batteries.
    pub(crate) fn get_battery_ids(&self) -> BTreeSet<u64> {
        self.groups
            .iter()
            .flat_map(|group| group.battery_ids.iter().copied())
            .collect()
    }

    /// Returns a formula for the active power of the battery pool.
    pub fn power(&mut self) -> Result<Formula<Power>, Error> {
        self.logical_meter
            .battery::<metric::AcPowerActive>(self.component_ids.clone())
    }

    /// Returns a formula for the usable capacity of the pool: the sum over
    /// its batteries of the capacity between the battery's SoC bounds.
    ///
    /// A battery contributes nothing while its capacity or SoC bounds are
    /// missing, while it is unhealthy, or while an inverter of its group is
    /// unhealthy or has sent nothing for `max_age_in_intervals` intervals. A
    /// battery whose bounds are equal or inverted contributes 0. The
    /// formula reads `None` when no battery contributes, and while any
    /// component of the pool is still being subscribed.
    pub fn capacity(&self) -> Formula<Energy> {
        self.logical_meter
            .formula_from_expr(battery_pool_formulas::usable_capacity(&self.groups))
    }

    /// Returns a formula for the state of charge of the pool: each
    /// battery's SoC, normalised to its SoC bounds and clamped to 0-100 %,
    /// weighted by the battery's usable capacity.
    ///
    /// A battery contributes nothing while its capacity, SoC or SoC bounds
    /// are missing, while it is unhealthy, or while an inverter of its group
    /// is unhealthy or has sent nothing for `max_age_in_intervals` intervals.
    /// A battery whose bounds are equal or inverted has no weight. The
    /// formula reads `None` when no battery contributes, and while any
    /// component of the pool is still being subscribed.
    pub fn soc(&self) -> Formula<Percentage> {
        self.logical_meter
            .formula_from_expr(battery_pool_formulas::soc(&self.groups))
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
            pool_bounds::compute_battery_pool_bounds::<metric::AcPowerActive, metric::DcPower>,
            format!(
                "{}/{}",
                metric::AcPowerActive::str_name(),
                metric::DcPower::str_name()
            ),
        );
        tokio::spawn(tracker.run());
        self.bounds_tx = Some(tx.downgrade());
        rx
    }

    /// Returns a receiver for a stream of [`BatteryPoolSnapshot`] values,
    /// each reflecting the latest component telemetry partitioned into
    /// healthy and unhealthy sets.
    ///
    /// Reuses the running tracker if one exists and still has active receivers
    /// (including any held by a bounds tracker); otherwise starts a new one.
    pub fn telemetry_snapshots(&mut self) -> broadcast::Receiver<BatteryPoolSnapshot> {
        if let Some(tx) = self
            .snapshot_tx
            .as_ref()
            .and_then(WeakCachingSender::upgrade)
            && tx.receiver_count() > 0
        {
            return tx.subscribe_with_current();
        }
        let tx = CachingSender::<BatteryPoolSnapshot>::new();
        // Subscribe before spawning so the tracker sees a receiver and doesn't
        // stop before this consumer has read anything.
        let rx = tx.subscribe_with_current();
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
            tx.clone(),
        );
        tokio::spawn(tracker.run());
        self.snapshot_tx = Some(tx.downgrade());
        rx
    }
}

#[cfg(test)]
mod tests {
    use super::BatteryPool;
    use crate::client::proto::common::microgrid::electrical_components::ElectricalComponentStateCode;
    use crate::client::test_utils::MockComponent;
    use crate::microgrid::test_utils::{handles, last_snapshot};
    use crate::{Formula, Sample, quantity::Quantity};
    use tokio_stream::{StreamExt, wrappers::BroadcastStream};

    /// grid → meter, with no batteries anywhere.
    fn battery_less_graph() -> MockComponent {
        MockComponent::grid(1).with_children(vec![MockComponent::meter(2)])
    }

    #[tokio::test]
    async fn try_new_none_constructs_an_empty_pool_without_batteries() {
        let (client, lm) = handles(battery_less_graph()).await;
        // A battery-less microgrid is a valid (empty) pool, not an error.
        let mut pool = BatteryPool::try_new(None, client, lm)
            .expect("a battery-less microgrid should yield an empty pool");
        pool.power().expect("empty pool power formula");
    }

    #[tokio::test(start_paused = true)]
    async fn empty_pool_emits_empty_snapshot_and_bounds() {
        let (client, lm) = handles(battery_less_graph()).await;
        let mut pool = BatteryPool::try_new(None, client, lm).unwrap();

        let mut snapshots = pool.telemetry_snapshots();
        let mut bounds = pool.power_bounds();

        let snapshot = last_snapshot(&mut snapshots, 5).await;
        assert!(
            snapshot.groups().is_empty(),
            "empty pool snapshot should have no groups, got {snapshot:?}"
        );

        let bounds = last_snapshot(&mut bounds, 5).await;
        assert!(
            bounds.is_empty(),
            "empty pool should have empty power bounds"
        );
    }

    /// grid → meter → battery_inverter(3) → [battery(4), battery(5)]
    fn shared_inverter_graph() -> MockComponent {
        MockComponent::grid(1).with_children(vec![MockComponent::meter(2).with_children(vec![
                MockComponent::battery_inverter(3).with_children(vec![
                    MockComponent::battery(4),
                    MockComponent::battery(5),
                ]),
            ])])
    }

    #[tokio::test]
    async fn try_new_rejects_partial_inverter_battery_group() {
        // Battery 4 shares inverter 3 with battery 5, so selecting only 4 is a
        // malformed selection. It must be rejected at construction rather than
        // silently surfacing later as an empty snapshot/bounds value.
        let (client, lm) = handles(shared_inverter_graph()).await;
        assert!(
            BatteryPool::try_new(Some([4].into()), client, lm).is_err(),
            "a partial inverter-battery group must be rejected"
        );
    }

    /// grid → meter → battery_inverter(3) → [battery(4), battery(5)], with
    /// 20 frames of telemetry: 4 at 1000 Wh, bounds 10..90 %, SoC 50 %;
    /// 5 at 2000 Wh, bounds 20..100 %, SoC 100 %.
    fn soc_graph(inverter_state: ElectricalComponentStateCode) -> MockComponent {
        MockComponent::grid(1).with_children(vec![MockComponent::meter(2).with_children(vec![
            MockComponent::battery_inverter(3)
                .with_power(vec![0.0; 20])
                .with_state(inverter_state)
                .with_children(vec![
                    MockComponent::battery(4)
                        .with_capacity(vec![1000.0; 20])
                        .with_soc(vec![50.0; 20], 10.0, 90.0),
                    MockComponent::battery(5)
                        .with_capacity(vec![2000.0; 20])
                        .with_soc(vec![100.0; 20], 20.0, 100.0),
                ]),
        ])])
    }

    /// The last of `count` samples the formula delivers. The first samples
    /// of a subscription may be `None` while components are still being
    /// subscribed, so tests read a few and keep the last.
    async fn last_sample<Q: Quantity + 'static>(formula: Formula<Q>, count: usize) -> Sample<Q> {
        let rx = formula.subscribe().await.unwrap();
        BroadcastStream::new(rx)
            .take(count)
            .map(|sample| sample.unwrap())
            .collect::<Vec<_>>()
            .await
            .pop()
            .unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn capacity_sums_the_usable_capacity_of_healthy_batteries() {
        let (client, lm) = handles(soc_graph(ElectricalComponentStateCode::Ready)).await;
        let pool = BatteryPool::try_new(None, client, lm).unwrap();
        let sample = last_sample(pool.capacity(), 4).await;
        assert_eq!(
            sample.value().map(|c| c.as_watthours()),
            Some(800.0 + 1600.0),
            "{}",
            pool.capacity()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn soc_is_weighted_by_usable_capacity() {
        let (client, lm) = handles(soc_graph(ElectricalComponentStateCode::Ready)).await;
        let pool = BatteryPool::try_new(None, client, lm).unwrap();
        let sample = last_sample(pool.soc(), 4).await;
        let soc = sample.value().map(|s| s.as_percentage()).unwrap();
        assert!((soc - 83.333336).abs() < 1e-3, "{soc}");
    }

    #[tokio::test(start_paused = true)]
    async fn soc_is_none_while_the_inverter_is_unhealthy() {
        let (client, lm) = handles(soc_graph(ElectricalComponentStateCode::Error)).await;
        let pool = BatteryPool::try_new(None, client, lm).unwrap();
        let sample = last_sample(pool.soc(), 4).await;
        assert_eq!(sample.value(), None);
    }
}
