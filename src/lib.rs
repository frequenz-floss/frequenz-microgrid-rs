// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! High-level interface for the Microgrid API.

mod bounds;
pub use bounds::Bounds;

pub mod client;
pub use client::MicrogridClientHandle;
pub(crate) use client::proto;

mod error;
pub use error::{Error, ErrorKind};

pub mod quantity;

mod sample;
pub use sample::Sample;

mod logical_meter;
pub use logical_meter::{Formula, FormulaSubscriber, LogicalMeterConfig, LogicalMeterHandle};

pub mod metric;
