// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Formula module for the logical meter.

use frequenz_microgrid_component_graph::Formula as _;
mod aggregation_formula;
mod coalesce_formula;
pub(crate) mod graph_formula_provider;
pub use aggregation_formula::AggregationFormula;
use async_trait::async_trait;
pub use coalesce_formula::CoalesceFormula;

use crate::{Error, Sample, metric::Metric, quantity::Quantity};
use tokio::sync::{broadcast, mpsc};

use super::logical_meter_actor;

/// Connects logical meter formulas to the component graph formulas.
pub(crate) trait GraphFormulaConnector: std::fmt::Display {
    type GraphFormulaType: frequenz_microgrid_component_graph::Formula;
}

#[async_trait]
pub trait FormulaSubscriber: std::fmt::Display + Sync + Send {
    type QuantityType: Quantity;
    async fn subscribe(&self) -> Result<broadcast::Receiver<Sample<Self::QuantityType>>, Error>;
}

/// Parameters for creating a logical meter formula.
pub(super) struct FormulaParams<F: GraphFormulaConnector, M: Metric> {
    pub(super) formula: F::GraphFormulaType,
    pub(super) metric: M,
    pub(super) instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
}

impl<F: GraphFormulaConnector, M: Metric> FormulaParams<F, M> {
    pub(super) fn new(
        formula: F::GraphFormulaType,
        metric: M,
        instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
    ) -> Self {
        Self {
            formula,
            metric,
            instructions_tx,
        }
    }
}

/// A trait that defines generic formula operations.
pub trait FormulaOps<Q: Quantity>:
    FormulaSubscriber<QuantityType = Q> + std::fmt::Display + Sized
{
    fn coalesce(self, other: Self) -> Result<Self, Error>;
    fn min(self, other: Self) -> Result<Self, Error>;
    fn max(self, other: Self) -> Result<Self, Error>;
}

impl<F, Q, M> FormulaOps<Q> for F
where
    F: FormulaSubscriber<QuantityType = Q>
        + GraphFormulaConnector
        + From<FormulaParams<F, M>>
        + Into<FormulaParams<F, M>>
        + std::fmt::Display,
    Q: Quantity,
    M: Metric<QuantityType = Q>,
{
    fn coalesce(self, other: Self) -> Result<Self, Error> {
        let mut params_self: FormulaParams<F, M> = self.into();
        let params_other: FormulaParams<F, M> = other.into();

        params_self.formula = params_self.formula.coalesce(params_other.formula);
        Ok(params_self.into())
    }

    fn min(self, other: Self) -> Result<Self, Error> {
        let mut params_self: FormulaParams<F, M> = self.into();
        let params_other: FormulaParams<F, M> = other.into();

        params_self.formula = params_self.formula.min(params_other.formula);
        Ok(params_self.into())
    }

    fn max(self, other: Self) -> Result<Self, Error> {
        let mut params_self: FormulaParams<F, M> = self.into();
        let params_other: FormulaParams<F, M> = other.into();

        params_self.formula = params_self.formula.max(params_other.formula);
        Ok(params_self.into())
    }
}
