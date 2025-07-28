// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! An formula that supports aggregation operations.

use super::{FormulaSubscriber, GraphFormulaProvider};
use crate::{
    Error, Sample, logical_meter::logical_meter_actor, proto::common::v1::metrics::Metric,
};
use tokio::sync::{broadcast, mpsc, oneshot};

#[derive(Clone)]
pub struct AggregationFormula {
    formula: frequenz_microgrid_component_graph::AggregationFormula,
    metric: Metric,
    instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
}

impl std::fmt::Display for AggregationFormula {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.formula.fmt(f)
    }
}

impl AggregationFormula {
    pub(crate) fn new(
        formula: frequenz_microgrid_component_graph::AggregationFormula,
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

impl GraphFormulaProvider for AggregationFormula {
    type GraphFormulaType = frequenz_microgrid_component_graph::AggregationFormula;
}

impl FormulaSubscriber for AggregationFormula {
    async fn subscribe(&self) -> Result<broadcast::Receiver<Sample>, Error> {
        let (tx, rx) = oneshot::channel();

        self.instructions_tx
            .send(logical_meter_actor::Instruction::SubscribeFormula {
                formula: self.formula.to_string(),
                metric: self.metric,
                response_tx: tx,
            })
            .await
            .map_err(|e| Error::connection_failure(format!("Could not send instruction: {e}")))?;
        let receiver = rx.await.map_err(|e| {
            Error::connection_failure(format!("Could not receive instruction: {e}"))
        })?;

        Ok(receiver)
    }
}

impl From<FormulaParams<AggregationFormula>> for AggregationFormula {
    fn from(params: FormulaParams<AggregationFormula>) -> Self {
        Self {
            formula: params.formula,
            metric: params.metric,
            instructions_tx: params.instructions_tx,
        }
    }
}

impl From<AggregationFormula> for FormulaParams<AggregationFormula> {
    fn from(formula: AggregationFormula) -> Self {
        FormulaParams {
            formula: formula.formula,
            metric: formula.metric,
            instructions_tx: formula.instructions_tx,
        }
    }
}

impl std::ops::Add for AggregationFormula {
    type Output = Result<Self, Error>;

    fn add(self, other: Self) -> Self::Output {
        if self.metric != other.metric {
            return Err(Error::invalid_metric(format!(
                "Cannot add formulas with different metrics: {} and {}",
                self.metric as isize, other.metric as isize
            )));
        }
        let new_formula = self.formula + other.formula;
        Ok(Self::new(new_formula, self.metric, self.instructions_tx))
    }
}

impl std::ops::Sub for AggregationFormula {
    type Output = Result<Self, Error>;

    fn sub(self, other: Self) -> Self::Output {
        if self.metric != other.metric {
            return Err(Error::invalid_metric(format!(
                "Cannot subtract formulas with different metrics: {} and {}",
                self.metric as isize, other.metric as isize
            )));
        }
        let new_formula = self.formula - other.formula;
        Ok(Self::new(new_formula, self.metric, self.instructions_tx))
    }
}

impl std::ops::Add<AggregationFormula> for Result<AggregationFormula, Error> {
    type Output = Result<AggregationFormula, Error>;

    fn add(self, other: AggregationFormula) -> Self::Output {
        match self {
            Ok(left) => left + other,
            Err(e) => Err(e),
        }
    }
}

impl std::ops::Sub<AggregationFormula> for Result<AggregationFormula, Error> {
    type Output = Result<AggregationFormula, Error>;

    fn sub(self, other: AggregationFormula) -> Self::Output {
        match self {
            Ok(left) => left - other,
            Err(e) => Err(e),
        }
    }
}

impl std::ops::Add<Result<AggregationFormula, Error>> for AggregationFormula {
    type Output = Result<AggregationFormula, Error>;

    fn add(self, other: Result<AggregationFormula, Error>) -> Self::Output {
        match other {
            Ok(right) => self + right,
            Err(e) => Err(e),
        }
    }
}

impl std::ops::Sub<Result<AggregationFormula, Error>> for AggregationFormula {
    type Output = Result<AggregationFormula, Error>;

    fn sub(self, other: Result<AggregationFormula, Error>) -> Self::Output {
        match other {
            Ok(right) => self - right,
            Err(e) => Err(e),
        }
    }
}
