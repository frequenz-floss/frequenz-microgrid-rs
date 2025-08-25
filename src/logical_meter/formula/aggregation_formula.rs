// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! An formula that supports aggregation operations.

use super::{FormulaParams, FormulaSubscriber, GraphFormulaProvider};
use crate::{
    Error, Sample, logical_meter::logical_meter_actor, metric::Metric, quantity::Quantity,
};
use tokio::sync::{broadcast, mpsc, oneshot};

#[derive(Clone)]
pub struct AggregationFormula<M: Metric> {
    formula: frequenz_microgrid_component_graph::AggregationFormula,
    metric: M,
    instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
}

impl<M: Metric> std::fmt::Display for AggregationFormula<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.formula.fmt(f)
    }
}

impl<M: Metric> GraphFormulaProvider for AggregationFormula<M> {
    type GraphFormulaType = frequenz_microgrid_component_graph::AggregationFormula;
}

impl<Q: Quantity + 'static, M: Metric<QuantityType = Q> + Sync> FormulaSubscriber
    for AggregationFormula<M>
{
    type MetricType = M;

    async fn subscribe(&self) -> Result<broadcast::Receiver<Sample<Q>>, Error> {
        let (tx, rx) = oneshot::channel();

        self.instructions_tx
            .send(logical_meter_actor::Instruction::SubscribeFormula {
                formula: self.formula.to_string(),
                metric: M::METRIC,
                response_tx: tx.try_into()?,
            })
            .await
            .map_err(|e| Error::connection_failure(format!("Could not send instruction: {e}")))?;
        let receiver = rx.await.map_err(|e| {
            Error::connection_failure(format!("Could not receive instruction: {e}"))
        })?;

        Ok(receiver)
    }
}

impl<M: Metric> From<FormulaParams<AggregationFormula<M>, M>> for AggregationFormula<M> {
    fn from(params: FormulaParams<AggregationFormula<M>, M>) -> Self {
        Self {
            formula: params.formula,
            metric: params.metric,
            instructions_tx: params.instructions_tx,
        }
    }
}

impl<M: Metric> From<AggregationFormula<M>> for FormulaParams<AggregationFormula<M>, M> {
    fn from(formula: AggregationFormula<M>) -> Self {
        FormulaParams {
            formula: formula.formula,
            metric: formula.metric,
            instructions_tx: formula.instructions_tx,
        }
    }
}

impl<M: Metric> std::ops::Add for AggregationFormula<M> {
    type Output = Result<Self, Error>;

    fn add(self, other: Self) -> Self::Output {
        if self.metric != other.metric {
            return Err(Error::invalid_metric(format!(
                "Cannot add formulas with different metrics: {} and {}",
                self.metric as isize, other.metric as isize
            )));
        }
        let new_formula = self.formula + other.formula;
        Ok(FormulaParams::new(new_formula, self.metric, self.instructions_tx).into())
    }
}

impl<M: Metric> std::ops::Sub for AggregationFormula<M> {
    type Output = Result<Self, Error>;

    fn sub(self, other: Self) -> Self::Output {
        if self.metric != other.metric {
            return Err(Error::invalid_metric(format!(
                "Cannot subtract formulas with different metrics: {} and {}",
                self.metric as isize, other.metric as isize
            )));
        }
        let new_formula = self.formula - other.formula;
        Ok(FormulaParams::new(new_formula, self.metric, self.instructions_tx).into())
    }
}

impl<M: Metric> std::ops::Add<AggregationFormula<M>> for Result<AggregationFormula<M>, Error> {
    type Output = Result<AggregationFormula<M>, Error>;

    fn add(self, other: AggregationFormula<M>) -> Self::Output {
        match self {
            Ok(left) => left + other,
            Err(e) => Err(e),
        }
    }
}

impl<M: Metric> std::ops::Sub<AggregationFormula<M>> for Result<AggregationFormula<M>, Error> {
    type Output = Result<AggregationFormula<M>, Error>;

    fn sub(self, other: AggregationFormula<M>) -> Self::Output {
        match self {
            Ok(left) => left - other,
            Err(e) => Err(e),
        }
    }
}

impl<M: Metric> std::ops::Add<Result<AggregationFormula<M>, Error>> for AggregationFormula<M> {
    type Output = Result<AggregationFormula<M>, Error>;

    fn add(self, other: Result<AggregationFormula<M>, Error>) -> Self::Output {
        match other {
            Ok(right) => self + right,
            Err(e) => Err(e),
        }
    }
}

impl<M: Metric> std::ops::Sub<Result<AggregationFormula<M>, Error>> for AggregationFormula<M> {
    type Output = Result<AggregationFormula<M>, Error>;

    fn sub(self, other: Result<AggregationFormula<M>, Error>) -> Self::Output {
        match other {
            Ok(right) => self - right,
            Err(e) => Err(e),
        }
    }
}
