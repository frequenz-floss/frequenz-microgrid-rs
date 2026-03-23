// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

use chrono::{DateTime, Utc};

/// Represents a measurement of a microgrid metric, made at a specific time.
#[derive(Copy, Clone, Debug, Default)]
pub struct Sample<Q: Copy + Clone + std::fmt::Debug + Default + std::fmt::Display> {
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) value: Option<Q>,
}

impl<Q: crate::quantity::Quantity> std::fmt::Display for Sample<Q> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sample({}, ", self.timestamp)?;

        if let Some(value) = self.value {
            write!(f, " {})", value)
        } else {
            write!(f, " None)")
        }
    }
}

impl<Q: Copy + Clone + Default + std::fmt::Debug + std::fmt::Display> frequenz_resampling::Sample
    for Sample<Q>
{
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

impl<Q: Copy + Clone + Default + std::fmt::Debug + std::fmt::Display> Sample<Q> {
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
