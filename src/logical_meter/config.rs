// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! This module defines the configuration for the logical meter.

use chrono::TimeDelta;

pub struct LogicalMeterConfig {
    /// The resampling interval for the logical meter.
    pub resampling_interval: TimeDelta,
}
