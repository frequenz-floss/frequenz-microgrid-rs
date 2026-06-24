// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Formula module for the logical meter.

use std::marker::PhantomData;

use async_trait::async_trait;
mod async_formula;
pub(crate) mod graph_formula;
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

#[async_trait]
pub trait FormulaSubscriber: std::fmt::Display + Sync + Send {
    type QuantityType: Quantity;
    async fn subscribe(&self) -> Result<broadcast::Receiver<Sample<Self::QuantityType>>, Error>;
}

/// Parameters for creating a logical meter formula.
pub(super) struct FormulaParams<M: Metric> {
    pub(super) formula: frequenz_microgrid_component_graph::Formula,
    pub(super) instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
    phantom: PhantomData<M>,
}

impl<M: Metric> FormulaParams<M> {
    pub(super) fn new(
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
