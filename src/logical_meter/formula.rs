// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Formula module for the logical meter.

use async_trait::async_trait;
mod async_formula;
pub(crate) mod graph_formula;
pub use async_formula::Formula;

use chrono::{DateTime, Utc};

use crate::{
    Error,
    Sample,
    client::proto::common::metrics::Metric as MetricPb,
    logical_meter::logical_meter_actor::FormulaSink,
    quantity::Quantity, //
};
use tokio::sync::broadcast;

/// Capacity of the per-subscriber broadcast channel carrying a formula's
/// samples.
const FORMULA_STREAM_CHANNEL_CAPACITY: usize = 100;

/// A component leaf of a formula expression: one metric of one component.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Key {
    /// The metric read from the component.
    pub metric: MetricPb,
    /// The component's id.
    pub component_id: u64,
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let metric = self.metric.as_str_name();
        let metric = metric.strip_prefix("METRIC_").unwrap_or(metric);
        write!(f, "{}:{}", self.component_id, metric)
    }
}

#[async_trait]
pub trait FormulaSubscriber: std::fmt::Display + Sync + Send {
    type QuantityType: Quantity;
    async fn subscribe(&self) -> Result<broadcast::Receiver<Sample<Self::QuantityType>>, Error>;
}

/// Delivers evaluated values to one subscriber as typed samples.
struct QuantitySink<Q: Quantity> {
    tx: broadcast::Sender<Sample<Q>>,
}

impl<Q: Quantity + 'static> FormulaSink for QuantitySink<Q> {
    fn send(&self, timestamp: DateTime<Utc>, value: Option<f32>) -> bool {
        self.tx
            .send(Sample::new(timestamp, value.map(Q::from_base_value)))
            .is_ok()
    }
}
