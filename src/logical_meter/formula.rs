// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Formula module for the logical meter.

use async_trait::async_trait;
mod async_formula;
pub(crate) mod graph_formula;
pub(crate) mod graph_formula_provider;
pub use async_formula::Formula;

use crate::{
    Error,
    Sample,
    quantity::Quantity, //
};
use tokio::sync::broadcast;

#[async_trait]
pub trait FormulaSubscriber: std::fmt::Display + Sync + Send {
    type QuantityType: Quantity;
    async fn subscribe(&self) -> Result<broadcast::Receiver<Sample<Self::QuantityType>>, Error>;
}
