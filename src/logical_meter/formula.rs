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
use frequenz_microgrid_formula_engine as engine;
use tokio::sync::{broadcast, mpsc};

use crate::{
    Error, Sample,
    client::proto::common::metrics::Metric as MetricPb,
    logical_meter::logical_meter_actor::{FormulaSink, Instruction},
    quantity::{Percentage, Quantity},
};

/// Capacity of the per-subscriber broadcast channel carrying a formula's
/// samples.
const FORMULA_STREAM_CHANNEL_CAPACITY: usize = 100;

/// A component leaf of a formula expression: one metric of one component.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Key {
    /// The metric read from the component.
    pub metric: MetricPb,
    /// The component's id.
    pub component_id: u64,
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let metric = self.metric.as_str_name();
        let metric = metric.strip_prefix("METRIC_").unwrap_or(metric);
        write!(f, "{}:{}", self.component_id, metric)
    }
}

/// A formula over component metrics, evaluated by the logical-meter actor once
/// per resampling tick.
///
/// Formulas compose with `+`, `-`, `* f32`, `/ f32`, `* Percentage`,
/// [`coalesce`](Self::coalesce), [`min`](Self::min), [`max`](Self::max) and
/// [`avg`](Self::avg). Composition never fails and never subscribes; only
/// [`subscribe`](Self::subscribe) does.
///
/// All formulas in one expression must come from the same logical meter, that
/// is one [`LogicalMeterHandle`](crate::LogicalMeterHandle) or its clones;
/// otherwise [`subscribe`](Self::subscribe) fails.
#[derive(Clone)]
pub struct Formula<Q: Quantity> {
    engine_formula: engine::Formula<f32, Key>,
    /// The logical meter that evaluates this formula; `None` when its
    /// operands come from different logical meters.
    instructions_tx: Option<mpsc::Sender<Instruction>>,
    _quantity: PhantomData<Q>,
}

/// Something a formula can be combined with: another formula of the same
/// quantity, or a constant.
pub enum FormulaOperand<Q: Quantity> {
    /// Another formula.
    Formula(Formula<Q>),
    /// A constant value.
    Constant(Q),
}

impl<Q: Quantity> From<Formula<Q>> for FormulaOperand<Q> {
    fn from(formula: Formula<Q>) -> Self {
        FormulaOperand::Formula(formula)
    }
}

impl<Q: Quantity> From<Q> for FormulaOperand<Q> {
    fn from(value: Q) -> Self {
        FormulaOperand::Constant(value)
    }
}

impl<Q: Quantity> Formula<Q> {
    pub(crate) fn new(
        engine_formula: engine::Formula<f32, Key>,
        instructions_tx: mpsc::Sender<Instruction>,
    ) -> Self {
        Self {
            engine_formula,
            instructions_tx: Some(instructions_tx),
            _quantity: PhantomData,
        }
    }

    /// The expression this formula evaluates.
    #[cfg(test)]
    pub(crate) fn engine_formula(&self) -> &engine::Formula<f32, Key> {
        &self.engine_formula
    }

    fn map(self, f: impl FnOnce(engine::Formula<f32, Key>) -> engine::Formula<f32, Key>) -> Self {
        Self {
            engine_formula: f(self.engine_formula),
            ..self
        }
    }

    /// Returns the operand's expression. A formula operand from another logical
    /// meter, or one that already mixes meters, clears this formula's logical
    /// meter, so `subscribe()` fails.
    fn take_operand(&mut self, operand: impl Into<FormulaOperand<Q>>) -> engine::Formula<f32, Key> {
        match operand.into() {
            FormulaOperand::Formula(formula) => {
                let same_meter = self
                    .instructions_tx
                    .as_ref()
                    .zip(formula.instructions_tx.as_ref())
                    .is_some_and(|(ours, theirs)| ours.same_channel(theirs));
                if !same_meter {
                    self.instructions_tx = None;
                }
                formula.engine_formula
            }
            FormulaOperand::Constant(value) => engine::Formula::Constant(Some(value.base_value())),
        }
    }

    fn combine(
        mut self,
        other: impl Into<FormulaOperand<Q>>,
        build: impl FnOnce(
            engine::Formula<f32, Key>,
            engine::Formula<f32, Key>,
        ) -> engine::Formula<f32, Key>,
    ) -> Self {
        let rhs = self.take_operand(other);
        self.map(|lhs| build(lhs, rhs))
    }

    /// `COALESCE(self, other)`: the first operand with a value. An operand
    /// whose components are still being subscribed is not skipped: the sample
    /// is `None` for that tick.
    pub fn coalesce(self, other: impl Into<FormulaOperand<Q>>) -> Self {
        self.combine(other, engine::Formula::coalesce)
    }

    /// `MIN(self, other)`. An operand with no value, missing or still being
    /// subscribed, makes the sample `None`.
    pub fn min(self, other: impl Into<FormulaOperand<Q>>) -> Self {
        self.combine(other, engine::Formula::min)
    }

    /// `MAX(self, other)`. An operand with no value, missing or still being
    /// subscribed, makes the sample `None`.
    pub fn max(self, other: impl Into<FormulaOperand<Q>>) -> Self {
        self.combine(other, engine::Formula::max)
    }

    /// `AVG(self, others...)`. Operands with no value are skipped; the sample
    /// is `None` when no operand has a value, or while an operand's components
    /// are still being subscribed.
    pub fn avg(mut self, others: Vec<impl Into<FormulaOperand<Q>>>) -> Self {
        let others: Vec<_> = others
            .into_iter()
            .map(|other| self.take_operand(other))
            .collect();
        self.map(|lhs| lhs.avg(others))
    }
}

impl<Q: Quantity + 'static> Formula<Q> {
    /// Starts streaming this formula's samples.
    ///
    /// Each call gets its own channel. Formulas with the same expression share
    /// one evaluation in the actor.
    ///
    /// Fails if the formula combines formulas from different logical meters.
    pub async fn subscribe(&self) -> Result<broadcast::Receiver<Sample<Q>>, Error> {
        let Some(instructions_tx) = &self.instructions_tx else {
            return Err(Error::formula_engine_error(
                "A formula cannot combine formulas from different logical meters",
            ));
        };
        let (tx, rx) = broadcast::channel(FORMULA_STREAM_CHANNEL_CAPACITY);
        instructions_tx
            .send(Instruction::SubscribeFormula {
                engine_formula: self.engine_formula.clone(),
                sink: Box::new(QuantitySink { tx }),
            })
            .await
            .map_err(|e| Error::connection_failure(format!("Could not send instruction: {e}")))?;
        Ok(rx)
    }
}

impl<Q: Quantity, R: Into<FormulaOperand<Q>>> std::ops::Add<R> for Formula<Q> {
    type Output = Self;

    fn add(self, rhs: R) -> Self {
        self.combine(rhs, |lhs, rhs| lhs + rhs)
    }
}

impl<Q: Quantity, R: Into<FormulaOperand<Q>>> std::ops::Sub<R> for Formula<Q> {
    type Output = Self;

    fn sub(self, rhs: R) -> Self {
        self.combine(rhs, |lhs, rhs| lhs - rhs)
    }
}

impl<Q: Quantity> std::ops::Mul<f32> for Formula<Q> {
    type Output = Self;

    fn mul(self, rhs: f32) -> Self {
        self.map(|lhs| lhs * engine::Formula::Constant(Some(rhs)))
    }
}

impl<Q: Quantity> std::ops::Div<f32> for Formula<Q> {
    type Output = Self;

    fn div(self, rhs: f32) -> Self {
        self.map(|lhs| lhs / engine::Formula::Constant(Some(rhs)))
    }
}

impl<Q: Quantity> std::ops::Mul<Percentage> for Formula<Q> {
    type Output = Self;

    fn mul(self, rhs: Percentage) -> Self {
        self * rhs.as_fraction()
    }
}

impl<Q: Quantity> std::fmt::Display for Formula<Q> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.engine_formula.fmt(f)
    }
}

/// Delivers evaluated values to one subscriber as typed samples.
struct QuantitySink<Q: Quantity> {
    tx: broadcast::Sender<Sample<Q>>,
}

impl<Q: Quantity + 'static> FormulaSink for QuantitySink<Q> {
    fn send(&self, timestamp: DateTime<Utc>, value: Option<f32>) -> bool {
        self.tx
            .send(Sample::new(timestamp, value.map(Q::from_base_value)))
            .is_ok()
    }
}
