// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Pool-level bounds aggregation for PV, steam boiler and battery pools.
//!
//! Each pool's healthy components have their per-metric bounds combined into a
//! single pool-level set. The PV inverters or steam boilers in a pool are wired
//! in parallel, so their bounds are simply added together. A battery pool
//! aggregates following the physical topology of its inverter-battery groups
//! (parallel within a side, series between the inverter and battery sides,
//! parallel across groups).

use crate::bounds::{combine_parallel_sets, intersect_bounds_sets};
use crate::client::proto::common::metrics::Bounds as PbBounds;
use crate::microgrid::bounds_aggregation::aggregate_parallel;
use crate::microgrid::telemetry_tracker::battery_pool_telemetry_tracker::BatteryPoolSnapshot;
use crate::microgrid::telemetry_tracker::pv_pool_telemetry_tracker::PvPoolSnapshot;
use crate::microgrid::telemetry_tracker::steam_boiler_pool_telemetry_tracker::SteamBoilerPoolSnapshot;
use crate::{Bounds, metric::Metric};

/// Aggregates the bounds of every healthy PV inverter in the pool. The
/// inverters are wired in parallel, so their bounds combine in parallel.
///
/// `M` is the metric used to read bounds from the PV inverters (e.g.
/// `AcPowerActive`).
pub(crate) fn compute_pv_pool_bounds<M>(status: &PvPoolSnapshot) -> Vec<Bounds<M::QuantityType>>
where
    M: Metric,
    Bounds<M::QuantityType>: From<PbBounds>,
{
    aggregate_parallel::<M>(&status.inverters.healthy)
}

/// Aggregates the bounds of every healthy steam boiler in the pool. The
/// boilers are wired in parallel, so their bounds combine in parallel.
///
/// `M` is the metric used to read bounds from the steam boilers (e.g.
/// `AcPowerActive`).
pub(crate) fn compute_steam_boiler_pool_bounds<M>(
    status: &SteamBoilerPoolSnapshot,
) -> Vec<Bounds<M::QuantityType>>
where
    M: Metric,
    Bounds<M::QuantityType>: From<PbBounds>,
{
    aggregate_parallel::<M>(&status.boilers.healthy)
}

/// Aggregates the power bounds of a battery pool following the physical
/// topology of its inverter-battery groups (see the module docs).
///
/// `InverterM` is the metric used to read bounds from inverters (e.g.
/// `AcPowerActive`); `BatteryM` is the metric used to read bounds from
/// batteries (e.g. `DcPower`). Both must share the same `QuantityType` so
/// their bounds can be intersected and summed.
pub(crate) fn compute_battery_pool_bounds<InverterM, BatteryM>(
    status: &BatteryPoolSnapshot,
) -> Vec<Bounds<InverterM::QuantityType>>
where
    InverterM: Metric,
    BatteryM: Metric<QuantityType = InverterM::QuantityType>,
    Bounds<InverterM::QuantityType>: From<PbBounds>,
{
    status
        .groups()
        .values()
        .map(|group| {
            let inverter_bounds = aggregate_parallel::<InverterM>(&group.inverters.healthy);
            let battery_bounds = aggregate_parallel::<BatteryM>(&group.batteries.healthy);
            intersect_bounds_sets(&inverter_bounds, &battery_bounds)
        })
        .fold(Vec::new(), |acc, group_bounds| {
            combine_parallel_sets(&acc, &group_bounds)
        })
}

#[cfg(test)]
mod pv_tests {
    use std::collections::HashMap;

    use crate::Bounds;
    use crate::client::proto::common::metrics::{
        Bounds as PbBounds, Metric as MetricPb, MetricSample,
    };
    use crate::client::proto::common::microgrid::electrical_components::ElectricalComponentTelemetry;
    use crate::metric::AcPowerActive;
    use crate::microgrid::telemetry_tracker::component_partition::ComponentHealthPartition;
    use crate::microgrid::telemetry_tracker::pv_pool_telemetry_tracker::PvPoolSnapshot;
    use crate::quantity::Power;

    use super::compute_pv_pool_bounds as compute_pool_bounds;
    use crate::microgrid::test_utils::telem_with_power_bounds;

    /// Builds a snapshot whose healthy set holds the given telemetry, keyed by
    /// component ID, and an empty unhealthy set.
    fn healthy_snapshot(healthy: Vec<ElectricalComponentTelemetry>) -> PvPoolSnapshot {
        let healthy = healthy
            .into_iter()
            .map(|t| (t.electrical_component_id, t))
            .collect();
        PvPoolSnapshot {
            inverters: ComponentHealthPartition {
                healthy,
                unhealthy: HashMap::new(),
            },
        }
    }

    #[test]
    fn single_inverter_uses_its_bounds() {
        let snap = healthy_snapshot(vec![telem_with_power_bounds(
            10,
            vec![(Some(-1000.0), Some(0.0))],
        )]);
        let bounds = compute_pool_bounds::<AcPowerActive>(&snap);
        assert_eq!(
            bounds,
            vec![Bounds::new(
                Some(Power::from_watts(-1000.0)),
                Some(Power::from_watts(0.0))
            )]
        );
    }

    #[test]
    fn parallel_inverters_add() {
        let snap = healthy_snapshot(vec![
            telem_with_power_bounds(10, vec![(Some(-1000.0), Some(0.0))]),
            telem_with_power_bounds(11, vec![(Some(-2000.0), Some(0.0))]),
        ]);
        let bounds = compute_pool_bounds::<AcPowerActive>(&snap);
        assert_eq!(
            bounds,
            vec![Bounds::new(
                Some(Power::from_watts(-3000.0)),
                Some(Power::from_watts(0.0))
            )]
        );
    }

    #[test]
    fn empty_pool_yields_empty_bounds() {
        let snap = healthy_snapshot(vec![]);
        let bounds = compute_pool_bounds::<AcPowerActive>(&snap);
        assert!(bounds.is_empty());
    }

    /// Only healthy inverters contribute to the pool bounds; unhealthy ones are
    /// ignored even when their last telemetry carried bounds.
    #[test]
    fn unhealthy_inverters_are_excluded() {
        let healthy = [telem_with_power_bounds(
            10,
            vec![(Some(-1000.0), Some(0.0))],
        )]
        .into_iter()
        .map(|t| (t.electrical_component_id, t))
        .collect();
        let mut unhealthy = HashMap::new();
        unhealthy.insert(
            11,
            Some(telem_with_power_bounds(
                11,
                vec![(Some(-9000.0), Some(0.0))],
            )),
        );
        let snap = PvPoolSnapshot {
            inverters: ComponentHealthPartition { healthy, unhealthy },
        };

        let bounds = compute_pool_bounds::<AcPowerActive>(&snap);
        assert_eq!(
            bounds,
            vec![Bounds::new(
                Some(Power::from_watts(-1000.0)),
                Some(Power::from_watts(0.0))
            )]
        );
    }

    /// An inverter that reports a different metric carries no active-power
    /// bounds, so it contributes nothing to the pool aggregate.
    #[test]
    fn inverter_without_matching_metric_contributes_nothing() {
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
        let snap = healthy_snapshot(vec![other]);
        let bounds = compute_pool_bounds::<AcPowerActive>(&snap);
        assert!(bounds.is_empty());
    }
}

#[cfg(test)]
mod battery_tests {
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
    use crate::microgrid::telemetry_tracker::component_partition::ComponentHealthPartition;
    use crate::microgrid::telemetry_tracker::inverter_battery_group_telemetry_tracker::InverterBatteryGroupStatus;
    use crate::quantity::Power;

    use super::compute_battery_pool_bounds as compute_pool_bounds;
    use crate::microgrid::test_utils::telem_with_power_bounds;

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
                inverters: ComponentHealthPartition {
                    healthy: healthy_inverters,
                    unhealthy: HashMap::new(),
                },
                batteries: ComponentHealthPartition {
                    healthy: healthy_batteries,
                    unhealthy: HashMap::new(),
                },
            },
        )]);

        let bounds = compute_pool_bounds::<AcPowerActive, AcPowerActive>(&snapshot);
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
                inverters: ComponentHealthPartition {
                    healthy: healthy_inverters,
                    unhealthy: HashMap::new(),
                },
                batteries: ComponentHealthPartition {
                    healthy: healthy_batteries,
                    unhealthy: HashMap::new(),
                },
            },
        )]);

        let bounds = compute_pool_bounds::<AcPowerActive, AcPowerActive>(&snapshot);
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
                    inverters: ComponentHealthPartition {
                        healthy: h_inv_1,
                        unhealthy: HashMap::new(),
                    },
                    batteries: ComponentHealthPartition {
                        healthy: h_bat_1,
                        unhealthy: HashMap::new(),
                    },
                },
            ),
            (
                g2,
                InverterBatteryGroupStatus {
                    inverters: ComponentHealthPartition {
                        healthy: h_inv_2,
                        unhealthy: HashMap::new(),
                    },
                    batteries: ComponentHealthPartition {
                        healthy: h_bat_2,
                        unhealthy: HashMap::new(),
                    },
                },
            ),
        ]);

        let bounds = compute_pool_bounds::<AcPowerActive, AcPowerActive>(&snapshot);
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
        let bounds = compute_pool_bounds::<AcPowerActive, AcPowerActive>(&snapshot);
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
                inverters: ComponentHealthPartition {
                    healthy: healthy_inverters,
                    unhealthy: HashMap::new(),
                },
                batteries: ComponentHealthPartition {
                    healthy: healthy_batteries,
                    unhealthy: HashMap::new(),
                },
            },
        )]);

        let bounds = compute_pool_bounds::<AcPowerActive, AcPowerActive>(&snapshot);
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
                inverters: ComponentHealthPartition {
                    healthy: healthy_inverters,
                    unhealthy: HashMap::new(),
                },
                batteries: ComponentHealthPartition {
                    healthy: healthy_batteries,
                    unhealthy: HashMap::new(),
                },
            },
        )]);

        let bounds = compute_pool_bounds::<AcPowerActive, AcPowerActive>(&snapshot);
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
                inverters: ComponentHealthPartition {
                    healthy: HashMap::new(),
                    unhealthy: unhealthy_inverters,
                },
                batteries: ComponentHealthPartition {
                    healthy: healthy_batteries,
                    unhealthy: HashMap::new(),
                },
            },
        )]);

        let bounds = compute_pool_bounds::<AcPowerActive, AcPowerActive>(&snapshot);
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
                inverters: ComponentHealthPartition {
                    healthy: healthy_inverters,
                    unhealthy: HashMap::new(),
                },
                batteries: ComponentHealthPartition {
                    healthy: HashMap::new(),
                    unhealthy: unhealthy_batteries,
                },
            },
        )]);

        let bounds = compute_pool_bounds::<AcPowerActive, AcPowerActive>(&snapshot);
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
                inverters: ComponentHealthPartition {
                    healthy: h_inv,
                    unhealthy: HashMap::new(),
                },
                batteries: ComponentHealthPartition {
                    healthy: h_bat,
                    unhealthy: HashMap::new(),
                },
            },
        )]);

        // Inverter side has no active-power bounds → group produces no
        // bounds, so the pool bounds are empty.
        let bounds = compute_pool_bounds::<AcPowerActive, AcPowerActive>(&snapshot);
        assert!(bounds.is_empty());
    }
}
