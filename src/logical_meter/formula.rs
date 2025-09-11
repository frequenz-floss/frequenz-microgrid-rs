// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Formula module for the logical meter.

use async_trait::async_trait;
pub(crate) mod aggregation_formula;
mod async_formula;
pub(crate) mod coalesce_formula;
pub(crate) mod graph_formula_provider;
pub use async_formula::Formula;

use crate::{
    Error, Sample, logical_meter::formula::async_formula::FormulaOperand, metric::Metric,
    quantity::Quantity,
};
use tokio::sync::{broadcast, mpsc};

use super::logical_meter_actor;

/// Connects logical meter formulas to the component graph formulas.
pub(crate) trait GraphFormulaConnector: std::fmt::Display {
    type GraphFormulaType: frequenz_microgrid_component_graph::Formula;
}

#[async_trait]
pub trait FormulaSubscriber: std::fmt::Display + Sync + Send {
    type QuantityType: Quantity;
    async fn subscribe(&self) -> Result<broadcast::Receiver<Sample<Self::QuantityType>>, Error>;
}

/// Parameters for creating a logical meter formula.
pub(super) struct FormulaParams<F: GraphFormulaConnector, M: Metric> {
    pub(super) formula: F::GraphFormulaType,
    pub(super) metric: M,
    pub(super) instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
}

impl<F: GraphFormulaConnector, M: Metric> FormulaParams<F, M> {
    pub(super) fn new(
        formula: F::GraphFormulaType,
        metric: M,
        instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
    ) -> Self {
        Self {
            formula,
            metric,
            instructions_tx,
        }
    }
}

// TODO: extend previous Coalesce instead of creating a new one, etc.
impl<Q> Formula<Q>
where
    Q: Quantity + 'static,
{
    pub fn coalesce(self, other: Formula<Q>) -> Result<Formula<Q>, Error> {
        match self {
            Formula::Coalesce(mut items) => {
                items.push(other.into());
                Ok(Formula::Coalesce(items))
            }
            _ => Ok(Formula::Coalesce(vec![
                FormulaOperand::Formula(Box::new(Formula::<Q, Q, f32>::Subscriber(Box::new(self)))),
                other.into(),
            ])),
        }
    }

    pub fn min(self, other: Formula<Q>) -> Result<Formula<Q>, Error> {
        match self {
            Formula::Min(mut items) => {
                items.push(other.into());
                Ok(Formula::Min(items))
            }
            _ => Ok(Formula::Min(vec![
                FormulaOperand::Formula(Box::new(Formula::<Q, Q, f32>::Subscriber(Box::new(self)))),
                other.into(),
            ])),
        }
    }

    pub fn max(self, other: Formula<Q>) -> Result<Formula<Q>, Error> {
        match self {
            Formula::Max(mut items) => {
                items.push(other.into());
                Ok(Formula::Max(items))
            }
            _ => Ok(Formula::Max(vec![
                FormulaOperand::Formula(Box::new(Formula::<Q, Q, f32>::Subscriber(Box::new(self)))),
                other.into(),
            ])),
        }
    }

    pub fn avg(self, others: Vec<Formula<Q>>) -> Result<Formula<Q>, Error> {
        let mut exprs: Vec<FormulaOperand<Q>> =
            vec![FormulaOperand::Formula(Box::new(
                Formula::<Q, Q, f32>::Subscriber(Box::new(self)),
            ))];
        for other in others {
            exprs.push(other.into());
        }
        Ok(Formula::Avg(exprs))
    }

    pub async fn subscribe(&self) -> Result<broadcast::Receiver<Sample<Q>>, Error> {
        <Self as FormulaSubscriber>::subscribe(self).await
    }
}

impl<Q, F> std::ops::Add<F> for Formula<Q>
where
    F: Into<FormulaOperand<Q>>,
    Q: Quantity + 'static,
{
    type Output = Formula<Q>;

    fn add(self, other: F) -> Self::Output {
        Formula::Add(vec![FormulaOperand::Formula(Box::new(self)), other.into()])
    }
}

impl<Q, F> std::ops::Sub<F> for Formula<Q>
where
    F: Into<FormulaOperand<Q>>,
    Q: Quantity + 'static,
{
    type Output = Formula<Q>;

    fn sub(self, other: F) -> Self::Output {
        Formula::Subtract(vec![FormulaOperand::Formula(Box::new(self)), other.into()])
    }
}

impl<Q> std::ops::Mul<f32> for Formula<Q, Q, f32>
where
    Q: Quantity + 'static,
{
    type Output = Formula<Q, Q, f32>;

    fn mul(self, other: f32) -> Self::Output {
        Formula::<Q, Q, f32>::Multiply(FormulaOperand::<Q>::Formula(Box::new(self)), other.into())
    }
}

impl<Q> std::ops::Div<f32> for Formula<Q, Q, f32>
where
    Q: Quantity + 'static,
{
    type Output = Formula<Q, Q, f32>;

    fn div(self, rhs: f32) -> Self::Output {
        Formula::<Q, Q, f32>::Divide(FormulaOperand::<Q>::Formula(Box::new(self)), rhs.into())
    }
}
