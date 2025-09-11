// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! An formula that supports aggregation operations.

use super::{FormulaParams, FormulaSubscriber, GraphFormulaConnector};
use crate::{
    Error, Sample, logical_meter::logical_meter_actor, metric::Metric, quantity::Quantity,
};
use async_trait::async_trait;
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

impl<M: Metric> GraphFormulaConnector for AggregationFormula<M> {
    type GraphFormulaType = frequenz_microgrid_component_graph::AggregationFormula;
}

#[async_trait]
impl<Q: Quantity + 'static, M: Metric<QuantityType = Q> + Sync> FormulaSubscriber<Q>
    for AggregationFormula<M>
{
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
