// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Formula module for the logical meter.

use std::marker::PhantomData;

use async_trait::async_trait;
pub(crate) mod aggregation_formula;
mod async_formula;
pub(crate) mod coalesce_formula;
pub(crate) mod graph_formula_provider;
pub use async_formula::Formula;

use crate::{
    Error,
    Sample,
    metric::Metric,
    quantity::Quantity, //
};
use tokio::sync::{
    broadcast,
    mpsc, //
};

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
    pub(super) instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
    phantom: PhantomData<M>,
}

impl<F: GraphFormulaConnector, M: Metric> FormulaParams<F, M> {
    pub(super) fn new(
        formula: F::GraphFormulaType,
        instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
    ) -> Self {
        Self {
            formula,
            instructions_tx,
            phantom: PhantomData,
        }
    }
}
