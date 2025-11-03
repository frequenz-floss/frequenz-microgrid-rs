// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

use chrono::{DateTime, Utc};

/// Represents a measurement of a microgrid metric, made at a specific time.
#[derive(Clone, Debug, Default)]
pub struct Sample<Q: Copy + Clone + std::fmt::Debug + Default> {
    timestamp: DateTime<Utc>,
    value: Option<Q>,
}

impl<Q: Copy + Clone + Default + std::fmt::Debug> frequenz_resampling::Sample for Sample<Q> {
    type Value = Q;

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

impl<Q: Copy + Clone + Default + std::fmt::Debug> Sample<Q> {
    /// Creates a new `Sample` with the given timestamp and value.
    pub fn new(timestamp: DateTime<Utc>, value: Option<Q>) -> Self {
        Self { timestamp, value }
    }

    /// Returns the timestamp of the sample.
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    /// Returns the value of the sample.
    pub fn value(&self) -> Option<Q> {
        self.value
    }
}
