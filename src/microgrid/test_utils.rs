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

/// Advances `steps` * 100ms of simulated time to let the producer task run,
/// then returns the most recent value the broadcast channel has delivered.
pub(crate) async fn last_snapshot<T: Clone>(
    rx: &mut tokio::sync::broadcast::Receiver<T>,
    steps: u32,
) -> T {
    use tokio::sync::broadcast::error::TryRecvError;
    for _ in 0..steps {
        tokio::time::advance(std::time::Duration::from_millis(100)).await;
    }
    let mut latest = None;
    loop {
        match rx.try_recv() {
            Ok(value) => latest = Some(value),
            Err(TryRecvError::Lagged(_)) => continue,
            Err(TryRecvError::Empty | TryRecvError::Closed) => break,
        }
    }
    latest.expect("a value should have been published")
}
