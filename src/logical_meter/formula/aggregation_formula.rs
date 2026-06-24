// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! An formula that supports aggregation operations.

use std::marker::PhantomData;

use super::{FormulaParams, FormulaSubscriber};
use crate::{
    Error, Sample, logical_meter::logical_meter_actor, metric::Metric, quantity::Quantity,
};
use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc, oneshot};

#[derive(Clone)]
pub struct AggregationFormula<M: Metric> {
    formula: frequenz_microgrid_component_graph::Formula,
    instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
    phantom: PhantomData<M>,
}

impl<M: Metric> std::fmt::Display for AggregationFormula<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}::({})", M::METRIC.as_str_name(), self.formula)
    }
}

#[async_trait]
impl<Q: Quantity + 'static, M: Metric<QuantityType = Q> + Sync + Send> FormulaSubscriber
    for AggregationFormula<M>
{
    type QuantityType = Q;

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

impl<M: Metric> From<FormulaParams<M>> for AggregationFormula<M> {
    fn from(params: FormulaParams<M>) -> Self {
        Self {
            formula: params.formula,
            instructions_tx: params.instructions_tx,
            phantom: PhantomData,
        }
    }
}

impl<M: Metric> From<AggregationFormula<M>> for FormulaParams<M> {
    fn from(formula: AggregationFormula<M>) -> Self {
        FormulaParams {
            formula: formula.formula,
            instructions_tx: formula.instructions_tx,
            phantom: PhantomData,
        }
    }
}
