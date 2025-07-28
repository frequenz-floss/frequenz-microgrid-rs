// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! An coalesce formula.

use super::{FormulaParams, FormulaSubscriber, GraphFormulaProvider};
use crate::{
    Error, Sample, logical_meter::logical_meter_actor, proto::common::v1::metrics::Metric,
};
use tokio::sync::{broadcast, mpsc, oneshot};

#[derive(Clone)]
pub struct CoalesceFormula {
    formula: frequenz_microgrid_component_graph::CoalesceFormula,
    metric: Metric,
    instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
}

impl std::fmt::Display for CoalesceFormula {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.formula.fmt(f)
    }
}

impl GraphFormulaProvider for CoalesceFormula {
    type GraphFormulaType = frequenz_microgrid_component_graph::CoalesceFormula;
}

impl FormulaSubscriber for CoalesceFormula {
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

impl From<FormulaParams<CoalesceFormula>> for CoalesceFormula {
    fn from(params: FormulaParams<CoalesceFormula>) -> Self {
        Self {
            formula: params.formula,
            metric: params.metric,
            instructions_tx: params.instructions_tx,
        }
    }
}

impl From<CoalesceFormula> for FormulaParams<CoalesceFormula> {
    fn from(formula: CoalesceFormula) -> Self {
        FormulaParams {
            formula: formula.formula,
            metric: formula.metric,
            instructions_tx: formula.instructions_tx,
        }
    }
}
