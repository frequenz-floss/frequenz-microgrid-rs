// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Bounds tracker for pools of microgrid components.
//!
//! Subscribes to a [`BatteryPoolSnapshot`] stream and, for each update, extracts
//! the bounds of a target metric from every healthy component and aggregates
//! them into a single pool-level set of bounds.
//!
//! Aggregation follows the physical topology of an inverter-battery group in
//! an `MxN` configuration (M inverters wired in parallel to N batteries wired
//! in parallel, with the inverter side in series with the battery side):
//!
//! * Healthy inverters within a group are in parallel — their bounds are
//!   added together.
//! * Healthy batteries within a group are in parallel — their bounds are
//!   added together.
//! * The inverter side and battery side of a group are in series — their
//!   aggregated bounds are intersected.
//! * Groups within a pool are in parallel — their bounds are added together.

use std::marker::PhantomData;

use tokio::sync::broadcast;

use crate::bounds::{combine_parallel_sets, intersect_bounds_sets};
use crate::client::proto::common::metrics::Bounds as PbBounds;
use crate::microgrid::bounds_aggregation::aggregate_parallel;
use crate::microgrid::telemetry_tracker::battery_pool_telemetry_tracker::BatteryPoolSnapshot;
use crate::{Bounds, metric::Metric};

/// Tracks and aggregates power bounds for a battery pool.
///
/// `InverterM` is the metric used to read bounds from inverters (e.g.
/// `AcPowerActive`); `BatteryM` is the metric used to read bounds from
/// batteries (e.g. `DcPower`). Both must share the same `QuantityType` so
/// their bounds can be intersected and summed.
pub(crate) struct BatteryPoolBoundsTracker<InverterM: Metric, BatteryM: Metric> {
    pool_status_rx: broadcast::Receiver<BatteryPoolSnapshot>,
    pool_bounds_tx: broadcast::Sender<Vec<Bounds<InverterM::QuantityType>>>,
    _marker: PhantomData<(InverterM, BatteryM)>,
}

impl<InverterM, BatteryM> BatteryPoolBoundsTracker<InverterM, BatteryM>
where
    InverterM: Metric,
    BatteryM: Metric<QuantityType = InverterM::QuantityType>,
    Bounds<InverterM::QuantityType>: From<PbBounds>,
{
    pub(crate) fn new(
        pool_status_rx: broadcast::Receiver<BatteryPoolSnapshot>,
        pool_bounds_tx: broadcast::Sender<Vec<Bounds<InverterM::QuantityType>>>,
    ) -> Self {
        Self {
            pool_status_rx,
            pool_bounds_tx,
            _marker: PhantomData,
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            match self.pool_status_rx.recv().await {
                Ok(pool_status) => {
                    let bounds = Self::compute_pool_bounds(&pool_status);
                    if self.pool_bounds_tx.send(bounds).is_err() {
                        tracing::debug!(
                            "No receivers for {}/{} bounds tracker; shutting down.",
                            InverterM::str_name(),
                            BatteryM::str_name(),
                        );
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(
                        "{}/{} bounds tracker lagged by {n} pool status updates.",
                        InverterM::str_name(),
                        BatteryM::str_name(),
                    );
                }
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::error!(
                        "Pool status channel closed; {}/{} bounds tracker shutting down.",
                        InverterM::str_name(),
                        BatteryM::str_name(),
                    );
                    break;
                }
            }
        }
    }

    fn compute_pool_bounds(status: &BatteryPoolSnapshot) -> Vec<Bounds<InverterM::QuantityType>> {
        status
            .groups()
            .values()
            .map(|group| {
                let inverter_bounds = aggregate_parallel::<InverterM>(&group.healthy_inverters);
                let battery_bounds = aggregate_parallel::<BatteryM>(&group.healthy_batteries);
                intersect_bounds_sets(&inverter_bounds, &battery_bounds)
            })
            .fold(Vec::new(), |acc, group_bounds| {
                combine_parallel_sets(&acc, &group_bounds)
            })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use crate::Bounds;
    use crate::client::proto::common::metrics::{
        Bounds as PbBounds, Metric as MetricPb, MetricSample,
    };
    use crate::client::proto::common::microgrid::electrical_components::ElectricalComponentTelemetry;
    use crate::metric::AcPowerActive;
    use crate::microgrid::telemetry_tracker::battery_pool_telemetry_tracker::{
        BatteryPoolSnapshot, InverterBatteryGroup,
    };
    use crate::microgrid::telemetry_tracker::inverter_battery_group_telemetry_tracker::InverterBatteryGroupStatus;
    use crate::quantity::Power;

    use super::BatteryPoolBoundsTracker;

    fn telem_with_power_bounds(
        id: u64,
        bounds: Vec<(Option<f32>, Option<f32>)>,
    ) -> ElectricalComponentTelemetry {
        ElectricalComponentTelemetry {
            electrical_component_id: id,
            metric_samples: vec![MetricSample {
                sample_time: None,
                metric: MetricPb::AcPowerActive as i32,
                value: None,
                bounds: bounds
                    .into_iter()
                    .map(|(lower, upper)| PbBounds { lower, upper })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn group(inverter_ids: &[u64], battery_ids: &[u64]) -> InverterBatteryGroup {
        InverterBatteryGroup::new(
            inverter_ids.iter().copied().collect::<BTreeSet<_>>(),
            battery_ids.iter().copied().collect::<BTreeSet<_>>(),
        )
    }

    fn status(
        groups: Vec<(InverterBatteryGroup, InverterBatteryGroupStatus)>,
    ) -> BatteryPoolSnapshot {
        BatteryPoolSnapshot::from_groups(groups.into_iter().collect())
    }

    #[test]
    fn single_group_intersects_inverter_and_battery_bounds() {
        let g = group(&[10], &[20]);
        let mut healthy_inverters = HashMap::new();
        healthy_inverters.insert(
            10,
            telem_with_power_bounds(
                10,
                vec![(Some(-1000.0), Some(-200.0)), (Some(200.0), Some(1000.0))],
            ),
        );
        let mut healthy_batteries = HashMap::new();
        healthy_batteries.insert(
            20,
            telem_with_power_bounds(20, vec![(Some(-500.0), Some(800.0))]),
        );

        let snapshot = status(vec![(
            g,
            InverterBatteryGroupStatus {
                healthy_inverters,
                healthy_batteries,
                unhealthy_inverters: HashMap::new(),
                unhealthy_batteries: HashMap::new(),
            },
        )]);

        let bounds = BatteryPoolBoundsTracker::<AcPowerActive, AcPowerActive>::compute_pool_bounds(
            &snapshot,
        );
        assert_eq!(
            bounds,
            vec![
                Bounds::new(
                    Some(Power::from_watts(-500.0)),
                    Some(Power::from_watts(-200.0))
                ),
                Bounds::new(
                    Some(Power::from_watts(200.0)),
                    Some(Power::from_watts(800.0))
                )
            ]
        );
    }

    #[test]
    fn parallel_inverters_add_within_group() {
        let g = group(&[10, 11], &[20]);
        let mut healthy_inverters = HashMap::new();
        healthy_inverters.insert(
            10,
            telem_with_power_bounds(10, vec![(Some(-1000.0), Some(1000.0))]),
        );
        healthy_inverters.insert(
            11,
            telem_with_power_bounds(11, vec![(Some(-2000.0), Some(2000.0))]),
        );
        let mut healthy_batteries = HashMap::new();
        // Wide battery bounds so the intersect doesn't clip
        healthy_batteries.insert(
            20,
            telem_with_power_bounds(20, vec![(Some(-10_000.0), Some(10_000.0))]),
        );

        let snapshot = status(vec![(
            g,
            InverterBatteryGroupStatus {
                healthy_inverters,
                healthy_batteries,
                unhealthy_inverters: HashMap::new(),
                unhealthy_batteries: HashMap::new(),
            },
        )]);

        let bounds = BatteryPoolBoundsTracker::<AcPowerActive, AcPowerActive>::compute_pool_bounds(
            &snapshot,
        );
        assert_eq!(
            bounds,
            vec![Bounds::new(
                Some(Power::from_watts(-3000.0)),
                Some(Power::from_watts(3000.0))
            )]
        );
    }

    #[test]
    fn multiple_groups_add_across_pool() {
        let g1 = group(&[10], &[20]);
        let mut h_inv_1 = HashMap::new();
        h_inv_1.insert(
            10,
            telem_with_power_bounds(10, vec![(Some(-1000.0), Some(1000.0))]),
        );
        let mut h_bat_1 = HashMap::new();
        h_bat_1.insert(
            20,
            telem_with_power_bounds(20, vec![(Some(-1000.0), Some(1000.0))]),
        );

        let g2 = group(&[11], &[21]);
        let mut h_inv_2 = HashMap::new();
        h_inv_2.insert(
            11,
            telem_with_power_bounds(11, vec![(Some(-500.0), Some(500.0))]),
        );
        let mut h_bat_2 = HashMap::new();
        h_bat_2.insert(
            21,
            telem_with_power_bounds(21, vec![(Some(-500.0), Some(500.0))]),
        );

        let snapshot = status(vec![
            (
                g1,
                InverterBatteryGroupStatus {
                    healthy_inverters: h_inv_1,
                    healthy_batteries: h_bat_1,
                    unhealthy_inverters: HashMap::new(),
                    unhealthy_batteries: HashMap::new(),
                },
            ),
            (
                g2,
                InverterBatteryGroupStatus {
                    healthy_inverters: h_inv_2,
                    healthy_batteries: h_bat_2,
                    unhealthy_inverters: HashMap::new(),
                    unhealthy_batteries: HashMap::new(),
                },
            ),
        ]);

        let bounds = BatteryPoolBoundsTracker::<AcPowerActive, AcPowerActive>::compute_pool_bounds(
            &snapshot,
        );
        assert_eq!(
            bounds,
            vec![Bounds::new(
                Some(Power::from_watts(-1500.0)),
                Some(Power::from_watts(1500.0))
            )]
        );
    }

    #[test]
    fn empty_pool_yields_empty_bounds() {
        let snapshot = status(vec![]);
        let bounds = BatteryPoolBoundsTracker::<AcPowerActive, AcPowerActive>::compute_pool_bounds(
            &snapshot,
        );
        assert!(bounds.is_empty());
    }

    /// When inverters have no power bounds (metric absent or empty `bounds`
    /// list), the group has no well-defined feasible region and must
    /// contribute no bounds to the pool aggregate.
    #[test]
    fn missing_inverter_bounds_yields_no_group_bounds() {
        let g = group(&[10], &[20]);

        // Inverter telemetry carries a matching metric but no bounds at all.
        let mut healthy_inverters = HashMap::new();
        healthy_inverters.insert(10, telem_with_power_bounds(10, vec![]));

        let mut healthy_batteries = HashMap::new();
        healthy_batteries.insert(
            20,
            telem_with_power_bounds(20, vec![(Some(-500.0), Some(500.0))]),
        );

        let snapshot = status(vec![(
            g,
            InverterBatteryGroupStatus {
                healthy_inverters,
                healthy_batteries,
                unhealthy_inverters: HashMap::new(),
                unhealthy_batteries: HashMap::new(),
            },
        )]);

        let bounds = BatteryPoolBoundsTracker::<AcPowerActive, AcPowerActive>::compute_pool_bounds(
            &snapshot,
        );
        assert!(
            bounds.is_empty(),
            "group with no inverter bounds must not contribute any bounds"
        );
    }

    /// Mirror of the above for the battery side: with batteries reporting no
    /// power bounds, the group must contribute no bounds to the pool.
    #[test]
    fn missing_battery_bounds_yields_no_group_bounds() {
        let g = group(&[10], &[20]);

        let mut healthy_inverters = HashMap::new();
        healthy_inverters.insert(
            10,
            telem_with_power_bounds(10, vec![(Some(-1000.0), Some(1000.0))]),
        );

        let mut healthy_batteries = HashMap::new();
        healthy_batteries.insert(20, telem_with_power_bounds(20, vec![]));

        let snapshot = status(vec![(
            g,
            InverterBatteryGroupStatus {
                healthy_inverters,
                healthy_batteries,
                unhealthy_inverters: HashMap::new(),
                unhealthy_batteries: HashMap::new(),
            },
        )]);

        let bounds = BatteryPoolBoundsTracker::<AcPowerActive, AcPowerActive>::compute_pool_bounds(
            &snapshot,
        );
        assert!(
            bounds.is_empty(),
            "group with no battery bounds must not contribute any bounds"
        );
    }

    /// If every inverter in the group is unhealthy, the group cannot dispatch
    /// power — the pool must report no bounds from this group regardless of
    /// what the healthy batteries could handle.
    #[test]
    fn no_healthy_inverters_yields_no_group_bounds() {
        let g = group(&[10], &[20]);

        let mut unhealthy_inverters = HashMap::new();
        unhealthy_inverters.insert(10, None);

        let mut healthy_batteries = HashMap::new();
        healthy_batteries.insert(
            20,
            telem_with_power_bounds(20, vec![(Some(-500.0), Some(500.0))]),
        );

        let snapshot = status(vec![(
            g,
            InverterBatteryGroupStatus {
                healthy_inverters: HashMap::new(),
                healthy_batteries,
                unhealthy_inverters,
                unhealthy_batteries: HashMap::new(),
            },
        )]);

        let bounds = BatteryPoolBoundsTracker::<AcPowerActive, AcPowerActive>::compute_pool_bounds(
            &snapshot,
        );
        assert!(
            bounds.is_empty(),
            "group with no healthy inverters must not contribute any bounds"
        );
    }

    /// Mirror of the above: no healthy batteries in the group means nothing
    /// to source/sink, so the group contributes no bounds to the pool.
    #[test]
    fn no_healthy_batteries_yields_no_group_bounds() {
        let g = group(&[10], &[20]);

        let mut healthy_inverters = HashMap::new();
        healthy_inverters.insert(
            10,
            telem_with_power_bounds(10, vec![(Some(-1000.0), Some(1000.0))]),
        );

        let mut unhealthy_batteries = HashMap::new();
        unhealthy_batteries.insert(20, None);

        let snapshot = status(vec![(
            g,
            InverterBatteryGroupStatus {
                healthy_inverters,
                healthy_batteries: HashMap::new(),
                unhealthy_inverters: HashMap::new(),
                unhealthy_batteries,
            },
        )]);

        let bounds = BatteryPoolBoundsTracker::<AcPowerActive, AcPowerActive>::compute_pool_bounds(
            &snapshot,
        );
        assert!(
            bounds.is_empty(),
            "group with no healthy batteries must not contribute any bounds"
        );
    }

    #[test]
    fn group_without_matching_metric_contributes_nothing() {
        let g = group(&[10], &[20]);
        // Telemetry exists but carries a different metric.
        let other = ElectricalComponentTelemetry {
            electrical_component_id: 10,
            metric_samples: vec![MetricSample {
                sample_time: None,
                metric: MetricPb::AcVoltage as i32,
                value: None,
                bounds: vec![PbBounds {
                    lower: Some(0.0),
                    upper: Some(1.0),
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut h_inv = HashMap::new();
        h_inv.insert(10, other);
        let mut h_bat = HashMap::new();
        h_bat.insert(
            20,
            telem_with_power_bounds(20, vec![(Some(-100.0), Some(100.0))]),
        );

        let snapshot = status(vec![(
            g,
            InverterBatteryGroupStatus {
                healthy_inverters: h_inv,
                healthy_batteries: h_bat,
                unhealthy_inverters: HashMap::new(),
                unhealthy_batteries: HashMap::new(),
            },
        )]);

        // Inverter side has no active-power bounds → group produces no
        // bounds, so the pool bounds are empty.
        let bounds = BatteryPoolBoundsTracker::<AcPowerActive, AcPowerActive>::compute_pool_bounds(
            &snapshot,
        );
        assert!(bounds.is_empty());
    }
}
