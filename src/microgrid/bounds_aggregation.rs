// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Helpers for aggregating per-component metric bounds into pool-level bounds.
//!
//! These are shared by the per-pool bounds trackers (e.g. battery and PV),
//! which read a target metric's bounds from each healthy component and combine
//! the components wired in parallel.

use std::collections::HashMap;

use crate::bounds::combine_parallel_sets;
use crate::client::proto::common::{
    metrics::Bounds as PbBounds, microgrid::electrical_components::ElectricalComponentTelemetry,
};
use crate::{Bounds, metric::Metric};

/// Combines the bounds of every component in the map as if they were wired
/// in parallel. Components that don't report the metric `M` are skipped.
pub(crate) fn aggregate_parallel<M: Metric>(
    components: &HashMap<u64, ElectricalComponentTelemetry>,
) -> Vec<Bounds<M::QuantityType>>
where
    Bounds<M::QuantityType>: From<PbBounds>,
{
    components
        .values()
        .filter_map(extract_metric_bounds::<M>)
        .fold(Vec::new(), |acc, bounds| {
            combine_parallel_sets(&acc, &bounds)
        })
}

fn extract_metric_bounds<M: Metric>(
    telemetry: &ElectricalComponentTelemetry,
) -> Option<Vec<Bounds<M::QuantityType>>>
where
    Bounds<M::QuantityType>: From<PbBounds>,
{
    telemetry.metric_samples.iter().find_map(|sample| {
        (sample.metric == M::METRIC as i32).then(|| {
            sample
                .bounds
                .iter()
                .map(|b| Bounds::from(*b))
                .collect::<Vec<_>>()
        })
    })
}
