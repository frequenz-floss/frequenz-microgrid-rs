// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! A representation of Bounds for any metric.

use crate::client::proto::common::metrics::Bounds as PbBounds;
use crate::quantity::{Current, Power, Quantity, ReactivePower};

/// A set of lower and upper bounds for any metric.
#[derive(Debug, Clone, PartialEq)]
pub struct Bounds<Q: Quantity> {
    /// The lower bound.
    /// If None, there is no lower bound.
    lower: Option<Q>,
    /// The upper bound.
    /// If None, there is no upper bound.
    upper: Option<Q>,
}

impl<Q: Quantity> std::fmt::Display for Bounds<Q> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "[{}, {}]",
            self.lower
                .map_or_else(|| String::from("None"), |x| x.to_string()),
            self.upper
                .map_or_else(|| String::from("None"), |x| x.to_string()),
        ))
    }
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

    /// Combines two bounds as if their components were connected in parallel.
    pub fn combine_parallel(&self, other: &Self) -> Vec<Self> {
        if self.intersect(other).is_none() {
            return vec![self.clone(), other.clone()];
        }
        // Lower side: if both lowers are ≤ 0, the components can both
        // discharge, so the combined floor is the sum (more negative).
        // Otherwise at least one range sits entirely above zero and the
        // combined floor is just the lower of the two individual floors.
        let lower = self.lower.and_then(|a| {
            other.lower.map(|b| {
                if a <= Q::zero() && b <= Q::zero() {
                    a + b
                } else {
                    a.min(b)
                }
            })
        });
        // Upper side: mirror of the above — both ≥ 0 means both can charge and
        // contributions add; otherwise take the higher of the two.
        let upper = self.upper.and_then(|a| {
            other.upper.map(|b| {
                if a >= Q::zero() && b >= Q::zero() {
                    a + b
                } else {
                    a.max(b)
                }
            })
        });
        vec![Bounds { lower, upper }]
    }

    /// Returns the intersection of `self` and `other`, or `None` if the
    /// intersection is empty.
    pub fn intersect(&self, other: &Self) -> Option<Self> {
        let lower = Self::map_or_any(Q::max, self.lower, other.lower);
        let upper = Self::map_or_any(Q::min, self.upper, other.upper);
        if let (Some(lower), Some(upper)) = (lower, upper)
            && lower > upper
        {
            return None;
        }
        Some(Bounds { lower, upper })
    }

    /// If `self` and `other` overlap, returns the smallest single interval
    /// that contains both; otherwise returns `None`.
    pub fn merge_if_overlapping(&self, other: &Self) -> Option<Self> {
        self.intersect(other)?;
        Some(Bounds {
            lower: self.lower.and_then(|a| other.lower.map(|b| a.min(b))),
            upper: self.upper.and_then(|a| other.upper.map(|b| a.max(b))),
        })
    }

    /// Combines two `Option<Q>` values with `f`, treating `None` as the
    /// identity: if exactly one side is `Some`, that value is returned
    /// unchanged. Only `(None, None)` yields `None`.
    fn map_or_any(f: impl FnOnce(Q, Q) -> Q, a: Option<Q>, b: Option<Q>) -> Option<Q> {
        match (a, b) {
            (Some(a), Some(b)) => Some(f(a, b)),
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        }
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

/// Combines two sets of bounds from components connected in parallel.
pub(crate) fn combine_parallel_sets<Q: Quantity>(
    a: &[Bounds<Q>],
    b: &[Bounds<Q>],
) -> Vec<Bounds<Q>> {
    match (a, b) {
        (a, []) | ([], a) => a.to_vec(),
        (a, b) => {
            let mut result = Vec::new();
            for b1 in a {
                for b2 in b {
                    result.extend(b1.combine_parallel(b2));
                }
            }
            squash_bounds_sets(result)
        }
    }
}

/// Intersects two sets of bounds together, returning the intersection of the
/// given sets.
///
/// This is used for calculating the combined bounds of two components connected
/// in series.
pub(crate) fn intersect_bounds_sets<Q: Quantity>(
    a: &[Bounds<Q>],
    b: &[Bounds<Q>],
) -> Vec<Bounds<Q>> {
    let mut result = Vec::new();
    for b1 in a {
        for b2 in b {
            if let Some(int) = b1.intersect(b2) {
                result.push(int);
            }
        }
    }
    squash_bounds_sets(result)
}

/// Merges overlapping bounds into disjoint intervals.
fn squash_bounds_sets<Q: Quantity>(mut input: Vec<Bounds<Q>>) -> Vec<Bounds<Q>> {
    if input.is_empty() {
        return input;
    }

    input.sort_by(|a, b| {
        a.lower
            .unwrap_or(Q::MIN)
            .partial_cmp(&b.lower.unwrap_or(Q::MIN))
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut squashed = Vec::new();
    let mut current = input[0].clone();

    for next in &input[1..] {
        if let Some(merged_bounds) = current.merge_if_overlapping(next) {
            current = merged_bounds;
        } else {
            squashed.push(current);
            current = next.clone();
        }
    }
    squashed.push(current);

    squashed
}

#[cfg(test)]
mod tests {
    use super::{Bounds, combine_parallel_sets, intersect_bounds_sets};
    use crate::quantity::Power;

    #[test]
    fn test_bounds_addition() {
        let b1 = Bounds::new(Some(-5.0), Some(5.0));
        let b2 = Bounds::new(Some(-3.0), Some(3.0));
        assert_eq!(
            b1.combine_parallel(&b2),
            vec![Bounds::new(Some(-8.0), Some(8.0))]
        );

        let b1 = Bounds::new(Some(-15.0), Some(-5.0));
        let b2 = Bounds::new(Some(-10.0), Some(-2.0));
        assert_eq!(
            b1.combine_parallel(&b2),
            vec![Bounds::new(Some(-25.0), Some(-2.0))]
        );

        let b1 = Bounds::new(Some(5.0), Some(15.0));
        let b2 = Bounds::new(Some(2.0), Some(10.0));
        assert_eq!(
            b1.combine_parallel(&b2),
            vec![Bounds::new(Some(2.0), Some(25.0))]
        );

        let b1 = Bounds::new(Some(5.0), Some(15.0));
        let b2 = Bounds::new(None, Some(10.0));
        assert_eq!(
            b1.combine_parallel(&b2),
            vec![Bounds::new(None, Some(25.0))]
        );

        let b1 = Bounds::new(Some(5.0), Some(15.0));
        let b2 = Bounds::new(Some(-5.0), None);
        assert_eq!(
            b1.combine_parallel(&b2),
            vec![Bounds::new(Some(-5.0), None)]
        );

        let b1 = Bounds::new(Some(5.0), Some(15.0));
        let b2 = Bounds::new(None, None);
        assert_eq!(b1.combine_parallel(&b2), vec![Bounds::new(None, None)]);

        let b1 = Bounds::new(Some(-10.0), Some(-5.0));
        let b2 = Bounds::new(Some(5.0), Some(15.0));
        assert_eq!(b1.combine_parallel(&b2), vec![b1, b2]);
    }

    #[test]
    fn test_combine_parallel_sets() {
        let b1 = vec![Bounds::new(Some(-5.0), Some(5.0))];
        let b2 = vec![
            Bounds::new(Some(-5.0), Some(-2.0)),
            Bounds::new(Some(2.0), Some(5.0)),
        ];
        let result = combine_parallel_sets(&b1, &b2);
        assert_eq!(result, vec![Bounds::new(Some(-10.0), Some(10.0))]);

        let b1 = vec![Bounds::new(Some(-5.0), Some(-1.0))];
        let b2 = vec![
            Bounds::new(Some(-5.0), Some(-2.0)),
            Bounds::new(Some(2.0), Some(5.0)),
        ];
        let result = combine_parallel_sets(&b1, &b2);
        assert_eq!(
            result,
            vec![
                Bounds::new(Some(-10.0), Some(-1.0)),
                Bounds::new(Some(2.0), Some(5.0))
            ]
        );
    }

    #[test]
    fn test_intersect_bounds_sets() {
        let vb1 = vec![
            Bounds::new(Some(-30.0), Some(-10.0)),
            Bounds::new(Some(10.0), Some(30.0)),
        ];
        let vb2 = vec![
            Bounds::new(Some(-20.0), Some(0.0)),
            Bounds::new(Some(20.0), Some(40.0)),
        ];
        let intersection = intersect_bounds_sets(&vb1, &vb2);
        assert_eq!(
            intersection,
            vec![
                Bounds::new(Some(-20.0), Some(-10.0)),
                Bounds::new(Some(20.0), Some(30.0)),
            ]
        );

        let vb2 = vec![
            Bounds::new(Some(-20.0), None),
            Bounds::new(None, Some(40.0)),
        ];
        let intersection = intersect_bounds_sets(&vb1, &vb2);
        assert_eq!(
            intersection,
            vec![
                Bounds::new(Some(-30.0), Some(-10.0)),
                Bounds::new(Some(10.0), Some(30.0)),
            ]
        );

        let vb2 = vec![
            Bounds::new(None, Some(-20.0)),
            Bounds::new(Some(20.0), None),
        ];
        let intersection = intersect_bounds_sets(&vb1, &vb2);
        assert_eq!(
            intersection,
            vec![
                Bounds::new(Some(-30.0), Some(-20.0)),
                Bounds::new(Some(20.0), Some(30.0)),
            ]
        );

        let vb2 = vec![Bounds::new(Some(-25.0), Some(25.0))];
        let intersection = intersect_bounds_sets(&vb1, &vb2);
        assert_eq!(
            intersection,
            vec![
                Bounds::new(Some(-25.0), Some(-10.0)),
                Bounds::new(Some(10.0), Some(25.0)),
            ]
        );

        let vb2 = vec![Bounds::new(Some(-5.0), Some(5.0))];
        let intersection = intersect_bounds_sets(&vb1, &vb2);
        assert_eq!(intersection, vec![]);
    }

    /// Bounds are closed intervals: intersecting at a shared endpoint yields a
    /// degenerate single-point interval rather than an empty result.
    #[test]
    fn intersect_single_point_is_non_empty() {
        let a = Bounds::new(Some(5.0), Some(10.0));
        let b = Bounds::new(Some(10.0), Some(15.0));
        assert_eq!(a.intersect(&b), Some(Bounds::new(Some(10.0), Some(10.0))));
    }

    /// Closed-interval semantics in `squash`: two intervals that touch at a
    /// single endpoint merge into one.
    #[test]
    fn squash_merges_touching_endpoints() {
        let a = [Bounds::new(Some(1.0), Some(5.0))];
        let b = [Bounds::new(Some(5.0), Some(10.0))];
        // `intersect_bounds_sets` runs the pairwise intersect through squash.
        let result = intersect_bounds_sets(
            &[Bounds::new(Some(0.0), Some(20.0))],
            &a.iter().chain(b.iter()).cloned().collect::<Vec<_>>(),
        );
        assert_eq!(result, vec![Bounds::new(Some(1.0), Some(10.0))]);
    }

    /// Fully-unbounded inputs are preserved through `combine_parallel`:
    /// `(−∞, ∞) ⊕ (−∞, ∞)` is still `(−∞, ∞)`, not empty.
    #[test]
    fn combine_parallel_preserves_fully_unbounded() {
        let a = Bounds::<f32>::new(None, None);
        let b = Bounds::<f32>::new(None, None);
        assert_eq!(a.combine_parallel(&b), vec![Bounds::new(None, None)]);
    }

    #[test]
    fn display_renders_both_bounds() {
        let b = Bounds::new(Some(-5.0_f32), Some(5.0_f32));
        assert_eq!(b.to_string(), "[-5, 5]");
    }

    #[test]
    fn display_renders_missing_lower_as_none() {
        let b = Bounds::new(None, Some(5.0_f32));
        assert_eq!(b.to_string(), "[None, 5]");
    }

    #[test]
    fn display_renders_missing_upper_as_none() {
        let b = Bounds::new(Some(-5.0_f32), None);
        assert_eq!(b.to_string(), "[-5, None]");
    }

    #[test]
    fn display_renders_fully_unbounded_as_none_none() {
        let b = Bounds::<f32>::new(None, None);
        assert_eq!(b.to_string(), "[None, None]");
    }

    #[test]
    fn display_uses_inner_quantity_formatting() {
        let b = Bounds::new(
            Some(Power::from_kilowatts(-1.0)),
            Some(Power::from_kilowatts(2.0)),
        );
        assert_eq!(b.to_string(), "[-1 kW, 2 kW]");
    }
}
