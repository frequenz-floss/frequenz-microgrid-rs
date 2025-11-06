// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Logical meter implementation for the Frequenz microgrid API.

mod config;
mod formula;
pub use formula::{Formula, FormulaSubscriber};

mod logical_meter_actor;
mod logical_meter_handle;
pub use logical_meter_handle::LogicalMeterHandle;
pub mod metric;

pub use config::LogicalMeterConfig;
