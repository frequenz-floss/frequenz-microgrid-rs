// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! A representation of Bounds for any metric.

use crate::client::proto::common::metrics::Bounds as PbBounds;
use crate::quantity::{Current, Power, Quantity, ReactivePower};

/// A set of lower and upper bounds for any metric.
pub struct Bounds<Q: Quantity> {
    /// The lower bound.
    /// If None, there is no lower bound.
    lower: Option<Q>,
    /// The upper bound.
    /// If None, there is no upper bound.
    upper: Option<Q>,
}

impl<Q: Quantity> Bounds<Q> {
    /// Creates a new `Bounds` with the given lower and upper bounds.
    pub fn new(lower: Option<Q>, upper: Option<Q>) -> Self {
        Self { lower, upper }
    }

    /// Returns the lower bound.
    pub fn lower(&self) -> Option<Q> {
        self.lower
    }

    /// Returns the upper bound.
    pub fn upper(&self) -> Option<Q> {
        self.upper
    }
}

impl<Q: Quantity> From<(Option<Q>, Option<Q>)> for Bounds<Q> {
    fn from(bounds: (Option<Q>, Option<Q>)) -> Self {
        Self::new(bounds.0, bounds.1)
    }
}

impl From<Bounds<Power>> for PbBounds {
    fn from(bounds: Bounds<Power>) -> Self {
        PbBounds {
            lower: bounds.lower.map(|q| q.as_watts()),
            upper: bounds.upper.map(|q| q.as_watts()),
        }
    }
}

impl From<Bounds<Current>> for PbBounds {
    fn from(bounds: Bounds<Current>) -> Self {
        PbBounds {
            lower: bounds.lower.map(|q| q.as_amperes()),
            upper: bounds.upper.map(|q| q.as_amperes()),
        }
    }
}

impl From<Bounds<ReactivePower>> for PbBounds {
    fn from(bounds: Bounds<ReactivePower>) -> Self {
        PbBounds {
            lower: bounds.lower.map(|q| q.as_volt_amperes_reactive()),
            upper: bounds.upper.map(|q| q.as_volt_amperes_reactive()),
        }
    }
}

impl From<PbBounds> for Bounds<Power> {
    fn from(pb_bounds: PbBounds) -> Self {
        Self::new(
            pb_bounds.lower.map(Power::from_watts),
            pb_bounds.upper.map(Power::from_watts),
        )
    }
}

impl From<PbBounds> for Bounds<Current> {
    fn from(pb_bounds: PbBounds) -> Self {
        Self::new(
            pb_bounds.lower.map(Current::from_amperes),
            pb_bounds.upper.map(Current::from_amperes),
        )
    }
}

impl From<PbBounds> for Bounds<ReactivePower> {
    fn from(pb_bounds: PbBounds) -> Self {
        Self::new(
            pb_bounds
                .lower
                .map(ReactivePower::from_volt_amperes_reactive),
            pb_bounds
                .upper
                .map(ReactivePower::from_volt_amperes_reactive),
        )
    }
}
