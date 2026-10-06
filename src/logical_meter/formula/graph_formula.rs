// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! A formula generated from the component graph, subscribable through the
//! logical-meter actor.

use std::marker::PhantomData;

use super::FormulaSubscriber;
use crate::{
    Error, Sample, logical_meter::logical_meter_actor, metric::Metric, quantity::Quantity,
};
use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc, oneshot};

/// A component-graph formula for metric `M`.
#[derive(Clone)]
pub struct GraphFormula<M: Metric> {
    formula: frequenz_microgrid_component_graph::Formula,
    instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
    phantom: PhantomData<fn() -> M>,
}

impl<M: Metric> std::fmt::Display for GraphFormula<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}::({})", M::METRIC.as_str_name(), self.formula)
    }
}

#[async_trait]
impl<Q: Quantity + 'static, M: Metric<QuantityType = Q>> FormulaSubscriber for GraphFormula<M> {
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

impl<M: Metric> GraphFormula<M> {
    /// Creates a formula that subscribes through the given actor channel.
    pub(crate) fn new(
        formula: frequenz_microgrid_component_graph::Formula,
        instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
    ) -> Self {
        Self {
            formula,
            instructions_tx,
            phantom: PhantomData,
        }
    }
}
