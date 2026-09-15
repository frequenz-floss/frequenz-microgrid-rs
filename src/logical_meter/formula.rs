// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Composable formulas over component metrics.
//!
//! A [`Formula`] is a typed wrapper around a formula-engine expression whose
//! component leaves are [`Key`]s. Composing formulas only builds the
//! expression; [`Formula::subscribe`] hands it to the logical-meter actor,
//! which evaluates it once per resampling tick.

use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use frequenz_microgrid_formula_engine::Formula as Expr;
use tokio::sync::{broadcast, mpsc};

use crate::{
    Error, Sample,
    client::proto::common::metrics::Metric as MetricPb,
    logical_meter::logical_meter_actor::{
        FORMULA_STREAM_CHANNEL_CAPACITY, FormulaSink, Instruction,
    },
    quantity::{Percentage, Quantity},
};

/// What a formula leaf reads from a component.
///
/// Bound and health leaves resample with the last value seen, not an
/// average; the default resampling function and its per-metric overrides
/// apply to metric values only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Source {
    /// The value of the component's sample for the metric.
    Value(MetricPb),
    /// The lower limit of the first bounds entry on the metric's sample.
    /// A sample with several bounds entries is read by its first entry
    /// only.
    LowerBound(MetricPb),
    /// The upper limit of the first bounds entry on the metric's sample.
    /// A sample with several bounds entries is read by its first entry
    /// only.
    UpperBound(MetricPb),
    /// 1 while the component's latest state is healthy, `None` otherwise.
    Health,
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::Value(metric) => f.write_str(metric_name(*metric)),
            Source::LowerBound(metric) => write!(f, "{}.lower", metric_name(*metric)),
            Source::UpperBound(metric) => write!(f, "{}.upper", metric_name(*metric)),
            Source::Health => f.write_str("HEALTH"),
        }
    }
}

/// The metric's proto name without its `METRIC_` prefix.
fn metric_name(metric: MetricPb) -> &'static str {
    let name = metric.as_str_name();
    name.strip_prefix("METRIC_").unwrap_or(name)
}

/// A component leaf of a formula expression: one source on one component.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Key {
    /// The component's id.
    pub component_id: u64,
    /// What is read from the component.
    pub source: Source,
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.component_id, self.source)
    }
}

/// The expression type behind every [`Formula`].
pub type FormulaExpr = Expr<f32, Key>;

/// A formula over component metrics, evaluated by the logical-meter actor
/// once per resampling tick.
///
/// Formulas are cheap to clone and compose with `+`, `-`, `* f32`, `/ f32`,
/// `* Percentage`, [`coalesce`](Self::coalesce), [`min`](Self::min),
/// [`max`](Self::max) and [`avg`](Self::avg). Composition never fails and
/// never subscribes; only [`subscribe`](Self::subscribe) does.
#[derive(Clone)]
pub struct Formula<Q: Quantity> {
    expr: FormulaExpr,
    instructions_tx: mpsc::Sender<Instruction>,
    _quantity: PhantomData<Q>,
}

/// Something a formula can be combined with: another formula of the same
/// quantity, or a constant.
pub enum Operand<Q: Quantity> {
    /// Another formula.
    Formula(Formula<Q>),
    /// A constant value.
    Constant(Q),
}

impl<Q: Quantity> Operand<Q> {
    fn into_expr(self) -> FormulaExpr {
        match self {
            Operand::Formula(formula) => formula.expr,
            Operand::Constant(value) => Expr::Constant(Some(value.base_value())),
        }
    }
}

impl<Q: Quantity> From<Formula<Q>> for Operand<Q> {
    fn from(formula: Formula<Q>) -> Self {
        Operand::Formula(formula)
    }
}

impl<Q: Quantity> From<Q> for Operand<Q> {
    fn from(value: Q) -> Self {
        Operand::Constant(value)
    }
}

impl<Q: Quantity> Formula<Q> {
    pub(crate) fn new(expr: FormulaExpr, instructions_tx: mpsc::Sender<Instruction>) -> Self {
        Self {
            expr,
            instructions_tx,
            _quantity: PhantomData,
        }
    }

    /// The expression this formula evaluates.
    pub fn expr(&self) -> &FormulaExpr {
        &self.expr
    }

    fn map_expr(self, f: impl FnOnce(FormulaExpr) -> FormulaExpr) -> Self {
        Self {
            expr: f(self.expr),
            ..self
        }
    }

    fn combine(
        self,
        other: impl Into<Operand<Q>>,
        build: impl FnOnce(FormulaExpr, FormulaExpr) -> FormulaExpr,
    ) -> Self {
        let rhs = other.into().into_expr();
        self.map_expr(|lhs| build(lhs, rhs))
    }

    /// `COALESCE(self, other)`: the first operand with a value.
    pub fn coalesce(self, other: impl Into<Operand<Q>>) -> Self {
        self.combine(other, Expr::coalesce)
    }

    /// `MIN(self, other)`.
    pub fn min(self, other: impl Into<Operand<Q>>) -> Self {
        self.combine(other, Expr::min)
    }

    /// `MAX(self, other)`.
    pub fn max(self, other: impl Into<Operand<Q>>) -> Self {
        self.combine(other, Expr::max)
    }

    /// `AVG(self, others...)`.
    pub fn avg(self, others: Vec<impl Into<Operand<Q>>>) -> Self {
        let others: Vec<_> = others
            .into_iter()
            .map(|other| other.into().into_expr())
            .collect();
        self.map_expr(|lhs| lhs.avg(others))
    }
}

impl<Q: Quantity + 'static> Formula<Q> {
    /// Starts streaming this formula's samples.
    ///
    /// Each call gets its own channel. Formulas with the same expression
    /// share one evaluation in the actor.
    pub async fn subscribe(&self) -> Result<broadcast::Receiver<Sample<Q>>, Error> {
        let (tx, rx) = broadcast::channel(FORMULA_STREAM_CHANNEL_CAPACITY);
        self.instructions_tx
            .send(Instruction::SubscribeFormula {
                expr: self.expr.clone(),
                sink: Box::new(TypedSink { tx }),
            })
            .await
            .map_err(|e| Error::connection_failure(format!("Could not send instruction: {e}")))?;
        Ok(rx)
    }
}

impl<Q: Quantity, R: Into<Operand<Q>>> std::ops::Add<R> for Formula<Q> {
    type Output = Self;

    fn add(self, rhs: R) -> Self {
        self.combine(rhs, |lhs, rhs| lhs + rhs)
    }
}

impl<Q: Quantity, R: Into<Operand<Q>>> std::ops::Sub<R> for Formula<Q> {
    type Output = Self;

    fn sub(self, rhs: R) -> Self {
        self.combine(rhs, |lhs, rhs| lhs - rhs)
    }
}

impl<Q: Quantity> std::ops::Mul<f32> for Formula<Q> {
    type Output = Self;

    fn mul(self, rhs: f32) -> Self {
        self.map_expr(|lhs| lhs * Expr::Constant(Some(rhs)))
    }
}

impl<Q: Quantity> std::ops::Div<f32> for Formula<Q> {
    type Output = Self;

    fn div(self, rhs: f32) -> Self {
        self.map_expr(|lhs| lhs / Expr::Constant(Some(rhs)))
    }
}

impl<Q: Quantity> std::ops::Mul<Percentage> for Formula<Q> {
    type Output = Self;

    fn mul(self, rhs: Percentage) -> Self {
        self.map_expr(|lhs| lhs * Expr::Constant(Some(rhs.as_fraction())))
    }
}

impl<Q: Quantity> std::fmt::Display for Formula<Q> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.expr.fmt(f)
    }
}

/// Delivers evaluated values to one subscriber as typed samples.
struct TypedSink<Q: Quantity> {
    tx: broadcast::Sender<Sample<Q>>,
}

impl<Q: Quantity + 'static> FormulaSink for TypedSink<Q> {
    fn send(&self, timestamp: DateTime<Utc>, value: Option<f32>) -> bool {
        self.tx
            .send(Sample::new(timestamp, value.map(Q::from_base_value)))
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::{Key, Source};
    use crate::client::proto::common::metrics::Metric as MetricPb;

    #[test]
    fn key_renders_component_and_source() {
        let key = Key {
            component_id: 5,
            source: Source::Value(MetricPb::AcPowerActive),
        };
        assert_eq!(key.to_string(), "5:AC_POWER_ACTIVE");
    }

    #[test]
    fn bound_and_health_sources_render_after_the_component() {
        let lower = Key {
            component_id: 5,
            source: Source::LowerBound(MetricPb::BatterySocPct),
        };
        let upper = Key {
            component_id: 5,
            source: Source::UpperBound(MetricPb::BatterySocPct),
        };
        let health = Key {
            component_id: 5,
            source: Source::Health,
        };
        assert_eq!(lower.to_string(), "5:BATTERY_SOC_PCT.lower");
        assert_eq!(upper.to_string(), "5:BATTERY_SOC_PCT.upper");
        assert_eq!(health.to_string(), "5:HEALTH");
    }
}
