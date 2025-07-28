// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Formula module for the logical meter.

mod aggregation_formula;
mod coalesce_formula;
pub(crate) mod graph_formula_provider;
pub use aggregation_formula::AggregationFormula;
pub use coalesce_formula::CoalesceFormula;

use crate::{Error, Sample, proto::common::v1::metrics::Metric};
use tokio::sync::{broadcast, mpsc};

use super::logical_meter_actor;

/// Connects logical meter formulas to the component graph formulas.
pub(crate) trait GraphFormulaProvider: std::fmt::Display {
    type GraphFormulaType: frequenz_microgrid_component_graph::Formula;
}

/// Defines a formula that can be subscribed to for receiving samples.
pub(crate) trait FormulaSubscriber: std::fmt::Display {
    fn subscribe(&self) -> impl Future<Output = Result<broadcast::Receiver<Sample>, Error>> + Send;
}

/// Parameters for creating a logical meter formula.
pub(super) struct FormulaParams<F: GraphFormulaProvider> {
    pub(super) formula: F::GraphFormulaType,
    pub(super) metric: Metric,
    pub(super) instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
}

impl<F: GraphFormulaProvider> FormulaParams<F> {
    pub(super) fn new(
        formula: F::GraphFormulaType,
        metric: Metric,
        instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
    ) -> Self {
        Self {
            formula,
            metric,
            instructions_tx,
        }
    }
}
