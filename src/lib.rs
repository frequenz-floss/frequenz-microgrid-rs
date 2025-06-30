// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! High-level interface for the Microgrid API.

mod client;
pub use client::MicrogridClientHandle;

mod error;
pub use error::{Error, ErrorKind};

mod proto;

mod sample;
pub use sample::Sample;

mod logical_meter;
pub use logical_meter::{Formula, LogicalMeterConfig, LogicalMeterHandle, Metric};
