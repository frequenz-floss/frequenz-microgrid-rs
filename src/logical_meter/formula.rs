// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Formula module for the logical meter.

mod aggregation_formula;
pub use aggregation_formula::AggregationFormula;

use crate::{Error, Sample};
use tokio::sync::broadcast;

/// Defines a formula that can be subscribed to for receiving samples.
pub trait Formula: std::fmt::Display {
    fn subscribe(&self) -> impl Future<Output = Result<broadcast::Receiver<Sample>, Error>> + Send;
}
