// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Shared test helpers for the pool modules.

use chrono::TimeDelta;

use crate::client::proto::common::metrics::{Bounds as PbBounds, Metric as MetricPb, MetricSample};
use crate::client::proto::common::microgrid::electrical_components::ElectricalComponentTelemetry;
use crate::client::test_utils::{MockComponent, MockMicrogridApiClient};
use crate::{LogicalMeterConfig, LogicalMeterHandle, MicrogridClientHandle};

/// Builds an [`ElectricalComponentTelemetry`] for `id` carrying a single
/// active-power sample whose `bounds` are the given `(lower, upper)` pairs.
pub(crate) fn telem_with_power_bounds(
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

/// Builds client and logical-meter handles backed by the given mock graph.
pub(crate) async fn handles(graph: MockComponent) -> (MicrogridClientHandle, LogicalMeterHandle) {
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

/// Drains `rx` for up to `steps` * 100ms of simulated time, returning the last
/// value seen. Panics if no value arrives.
pub(crate) async fn last_snapshot<T: Clone>(
    rx: &mut tokio::sync::broadcast::Receiver<T>,
    steps: u32,
) -> T {
    let mut last = None;
    for _ in 0..steps {
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
        while let Ok(snap) = rx.try_recv() {
            last = Some(snap);
        }
    }
    last.expect("no snapshot received")
}
