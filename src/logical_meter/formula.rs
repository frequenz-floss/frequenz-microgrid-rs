// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Formula module for the logical meter.

use frequenz_microgrid_component_graph::Formula as _;
mod aggregation_formula;
mod coalesce_formula;
pub(crate) mod graph_formula_provider;
pub use aggregation_formula::AggregationFormula;
pub use coalesce_formula::CoalesceFormula;

use crate::{Error, Sample, proto::common::v1alpha8::metrics::Metric};
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

/// A trait that defines generic formula operations.
pub trait Formula: std::fmt::Display + Sized {
    fn coalesce(self, other: Self) -> Result<Self, Error>;
    fn min(self, other: Self) -> Result<Self, Error>;
    fn max(self, other: Self) -> Result<Self, Error>;
    fn subscribe(&self) -> impl Future<Output = Result<broadcast::Receiver<Sample>, Error>> + Send;
}

impl<T> Formula for T
where
    T: FormulaSubscriber
        + GraphFormulaProvider
        + From<FormulaParams<T>>
        + Into<FormulaParams<T>>
        + std::fmt::Display,
{
    fn coalesce(self, other: Self) -> Result<Self, Error> {
        let mut params_self: FormulaParams<T> = self.into();
        let params_other: FormulaParams<T> = other.into();

        if params_self.metric != params_other.metric {
            return Err(Error::invalid_metric(format!(
                "Cannot coalesce formulas with different metrics: {} and {}",
                params_self.metric.as_str_name(),
                params_other.metric.as_str_name()
            )));
        }
        params_self.formula = params_self.formula.coalesce(params_other.formula);
        Ok(params_self.into())
    }

    fn min(self, other: Self) -> Result<Self, Error> {
        let mut params_self: FormulaParams<T> = self.into();
        let params_other: FormulaParams<T> = other.into();

        if params_self.metric != params_other.metric {
            return Err(Error::invalid_metric(format!(
                "Cannot take min of formulas with different metrics: {} and {}",
                params_self.metric.as_str_name(),
                params_other.metric.as_str_name()
            )));
        }
        params_self.formula = params_self.formula.min(params_other.formula);
        Ok(params_self.into())
    }

    fn max(self, other: Self) -> Result<Self, Error> {
        let mut params_self: FormulaParams<T> = self.into();
        let params_other: FormulaParams<T> = other.into();

        if params_self.metric != params_other.metric {
            return Err(Error::invalid_metric(format!(
                "Cannot take max of formulas with different metrics: {} and {}",
                params_self.metric.as_str_name(),
                params_other.metric.as_str_name()
            )));
        }
        params_self.formula = params_self.formula.max(params_other.formula);
        Ok(params_self.into())
    }

    fn subscribe(&self) -> impl Future<Output = Result<broadcast::Receiver<Sample>, Error>> + Send {
        <T as FormulaSubscriber>::subscribe(self)
    }
}
