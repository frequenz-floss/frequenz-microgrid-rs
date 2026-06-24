// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! A formula generated from the component graph, subscribable through the
//! logical-meter actor.
//!
//! The wrapper body is identical for every formula; the marker type `K` only
//! selects which component-graph methods generate it (the aggregation
//! `*_formula` methods vs. the `*_coalesce_formula` ones), via the
//! [`GraphFormulaProvider`](super::graph_formula_provider::GraphFormulaProvider)
//! impls. The [`AggregationFormula`] / [`CoalesceFormula`] aliases name the two
//! kinds.

use std::marker::PhantomData;

use super::{FormulaParams, FormulaSubscriber};
use crate::{
    Error, Sample, logical_meter::logical_meter_actor, metric::Metric, quantity::Quantity,
};
use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc, oneshot};

/// Marker for formulas from the aggregation (`*_formula`) graph methods.
pub enum Aggregation {}

/// Marker for formulas from the coalesce (`*_coalesce_formula`) graph methods.
pub enum Coalesce {}

/// A component-graph formula for metric `M`, tagged by the kind `K` that
/// selects the graph methods generating it.
pub struct GraphFormula<M: Metric, K> {
    formula: frequenz_microgrid_component_graph::Formula,
    instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
    phantom: PhantomData<(M, K)>,
}

/// A formula that supports aggregation operations.
pub type AggregationFormula<M> = GraphFormula<M, Aggregation>;

/// A formula built from the component graph's coalesce methods.
pub type CoalesceFormula<M> = GraphFormula<M, Coalesce>;

// Manual `Clone`: a derive would demand `K: Clone`, but the marker types are
// uninhabited and carry no data.
impl<M: Metric, K> Clone for GraphFormula<M, K> {
    fn clone(&self) -> Self {
        Self {
            formula: self.formula.clone(),
            instructions_tx: self.instructions_tx.clone(),
            phantom: PhantomData,
        }
    }
}

impl<M: Metric, K> std::fmt::Display for GraphFormula<M, K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}::({})", M::METRIC.as_str_name(), self.formula)
    }
}

#[async_trait]
impl<Q: Quantity + 'static, M: Metric<QuantityType = Q> + Sync + Send, K: Sync + Send>
    FormulaSubscriber for GraphFormula<M, K>
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

impl<M: Metric, K> From<FormulaParams<M>> for GraphFormula<M, K> {
    fn from(params: FormulaParams<M>) -> Self {
        Self {
            formula: params.formula,
            instructions_tx: params.instructions_tx,
            phantom: PhantomData,
        }
    }
}
