// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Methods for composing formulas.

use tokio::sync::broadcast;

use crate::{
    Error,
    Sample,
    logical_meter::formula::{
        Formula,
        FormulaSubscriber,
        async_formula::FormulaOperand, //
    },
    quantity::Quantity, //
};

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
