// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

use chrono::{DateTime, Utc};

/// Represents a measurement of a microgrid metric, made at a specific time.
#[derive(Clone, Debug, Default)]
pub struct Sample {
    timestamp: DateTime<Utc>,
    value: Option<f32>,
}

impl frequenz_resampling::Sample for Sample {
    type Value = f32;

    fn new(timestamp: DateTime<Utc>, value: Option<Self::Value>) -> Self {
        Self { timestamp, value }
    }

    fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    fn value(&self) -> Option<Self::Value> {
        self.value
    }
}

impl Sample {
    /// Creates a new `Sample` with the given timestamp and value.
    pub fn new(timestamp: DateTime<Utc>, value: Option<f32>) -> Self {
        Self { timestamp, value }
    }

    /// Returns the timestamp of the sample.
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    /// Returns the value of the sample.
    pub fn value(&self) -> Option<f32> {
        self.value
    }
}
