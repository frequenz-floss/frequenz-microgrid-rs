// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Formula module for the logical meter.

mod aggregation_formula;
mod coalesce_formula;
pub(crate) mod graph_formula_provider;
pub use aggregation_formula::AggregationFormula;
pub use coalesce_formula::CoalesceFormula;

use crate::{Error, Sample};
use tokio::sync::broadcast;

/// Defines a formula that can be subscribed to for receiving samples.
pub(crate) trait FormulaSubscriber: std::fmt::Display {
    fn subscribe(&self) -> impl Future<Output = Result<broadcast::Receiver<Sample>, Error>> + Send;
}
