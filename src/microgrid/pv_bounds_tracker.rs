// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Bounds tracker for a pool of PV inverters.
//!
//! Subscribes to a [`PvPoolSnapshot`] stream and, for each update, extracts the
//! bounds of a target metric from every healthy PV inverter and aggregates them
//! into a single pool-level set of bounds.
//!
//! PV inverters in a pool are wired in parallel, so their bounds are simply
//! added together.

use crate::client::proto::common::metrics::Bounds as PbBounds;
use crate::microgrid::bounds_aggregation::aggregate_parallel;
use crate::microgrid::telemetry_tracker::pv_pool_telemetry_tracker::PvPoolSnapshot;
use crate::{Bounds, metric::Metric};

/// Aggregates the bounds of every healthy PV inverter in the pool. The
/// inverters are wired in parallel, so their bounds combine in parallel.
///
/// `M` is the metric used to read bounds from the PV inverters (e.g.
/// `AcPowerActive`).
pub(crate) fn compute_pool_bounds<M>(status: &PvPoolSnapshot) -> Vec<Bounds<M::QuantityType>>
where
    M: Metric,
    Bounds<M::QuantityType>: From<PbBounds>,
{
    aggregate_parallel::<M>(&status.healthy_inverters)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::Bounds;
    use crate::client::proto::common::metrics::{
        Bounds as PbBounds, Metric as MetricPb, MetricSample,
    };
    use crate::client::proto::common::microgrid::electrical_components::ElectricalComponentTelemetry;
    use crate::metric::AcPowerActive;
    use crate::microgrid::telemetry_tracker::pv_pool_telemetry_tracker::PvPoolSnapshot;
    use crate::quantity::Power;

    use super::compute_pool_bounds;

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

    /// Builds a snapshot whose healthy set holds the given telemetry, keyed by
    /// component ID, and an empty unhealthy set.
    fn healthy_snapshot(healthy: Vec<ElectricalComponentTelemetry>) -> PvPoolSnapshot {
        let healthy = healthy
            .into_iter()
            .map(|t| (t.electrical_component_id, t))
            .collect();
        PvPoolSnapshot {
            healthy_inverters: healthy,
            unhealthy_inverters: HashMap::new(),
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
            healthy_inverters: healthy,
            unhealthy_inverters: unhealthy,
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
