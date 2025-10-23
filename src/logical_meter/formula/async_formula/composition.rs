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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{client::test_utils::logging::capture_logs, quantity::Power};
    use async_trait::async_trait;
    use chrono::{DateTime, TimeDelta, Utc};

    #[tokio::test]
    async fn test_addition() {
        let composed = formula(1, vec![Some(10.0), Some(12.0), None, Some(20.0)])
            + formula(2, vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)])
            + formula(3, vec![Some(2.0), Some(3.0), Some(4.0), Some(5.0)]);
        assert_eq!(composed.to_string(), "((#1 + #2) + #3)");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![Some(13.0), Some(17.0), None, Some(29.0)]
        );

        let composed = formula(4, vec![None, Some(100.0), Some(-10.0), Some(5.0)]) + composed;
        assert_eq!(composed.to_string(), "(#4 + ((#1 + #2) + #3))");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![None, Some(117.0), None, Some(34.0)]
        );

        let composed = formula(5, vec![None, Some(4.0), Some(-10.0), Some(-10.0)]) + composed;
        assert_eq!(composed.to_string(), "(#5 + (#4 + ((#1 + #2) + #3)))");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![None, Some(121.0), None, Some(24.0)]
        );

        let composed = formula(1, vec![Some(10.0), Some(12.0), None, Some(20.0)])
            + Power::from_watts(5.0)
            + formula(2, vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);
        assert_eq!(composed.to_string(), "((#1 + 5 W) + #2)");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![Some(16.0), Some(19.0), None, Some(29.0)]
        );
    }

    #[tokio::test]
    async fn test_subtraction() {
        let composed = formula(1, vec![Some(10.0), Some(12.0), None, Some(20.0)])
            - formula(2, vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)])
            - formula(3, vec![Some(2.0), Some(3.0), Some(4.0), Some(5.0)]);
        assert_eq!(composed.to_string(), "((#1 - #2) - #3)");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![Some(7.0), Some(7.0), None, Some(11.0)]
        );

        let composed = formula(4, vec![None, Some(100.0), Some(-10.0), Some(5.0)]) - composed;
        assert_eq!(composed.to_string(), "(#4 - ((#1 - #2) - #3))");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![None, Some(93.0), None, Some(-6.0)]
        );

        let composed = formula(5, vec![None, Some(4.0), Some(-10.0), Some(-10.0)]) - composed;
        assert_eq!(composed.to_string(), "(#5 - (#4 - ((#1 - #2) - #3)))");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![None, Some(-89.0), None, Some(-4.0)]
        );

        let composed = formula(1, vec![Some(10.0), Some(12.0), None, Some(20.0)])
            - Power::from_watts(5.0)
            - formula(2, vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);
        assert_eq!(composed.to_string(), "((#1 - 5 W) - #2)");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![Some(4.0), Some(5.0), None, Some(11.0)]
        );
    }

    #[tokio::test]
    async fn test_multiplication() {
        let composed = formula(1, vec![Some(10.0), Some(12.0), None, Some(20.0)]) * 2.0;
        assert_eq!(composed.to_string(), "(#1 * 2)");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![Some(20.0), Some(24.0), None, Some(40.0)]
        );
    }

    #[tokio::test]
    async fn test_division() {
        let composed = formula(1, vec![Some(10.0), Some(12.0), None, Some(20.0)]) / 2.0;
        assert_eq!(composed.to_string(), "(#1 / 2)");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![Some(5.0), Some(6.0), None, Some(10.0)]
        );

        let composed = formula(1, vec![Some(10.0), Some(12.0), None, Some(20.0)]) / 0.0;
        assert_eq!(composed.to_string(), "(#1 / 0)");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![None, None, None, None]
        );
    }

    #[tokio::test]
    async fn test_coalesce() {
        let composed = formula(1, vec![None, Some(12.0), None, Some(20.0)])
            .coalesce(formula(2, vec![Some(1.0), None, None, Some(4.0)]))
            .unwrap()
            .coalesce(formula(3, vec![Some(2.0), Some(3.0), Some(8.0), None]))
            .unwrap();

        assert_eq!(composed.to_string(), "COALESCE(#1, #2, #3)");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![Some(1.0), Some(12.0), Some(8.0), Some(20.0)]
        );
    }

    #[tokio::test]
    async fn test_min() {
        let composed = formula(1, vec![Some(10.0), Some(1.0), None, Some(20.0)])
            .min(formula(
                2,
                vec![Some(5.0), Some(15.0), Some(3.0), Some(25.0)],
            ))
            .unwrap()
            .min(formula(
                3,
                vec![Some(8.0), Some(11.0), Some(4.0), Some(18.0)],
            ))
            .unwrap();

        assert_eq!(composed.to_string(), "MIN(#1, #2, #3)");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![Some(5.0), Some(1.0), None, Some(18.0)]
        );
    }

    #[tokio::test]
    async fn test_max() {
        let composed = formula(1, vec![Some(10.0), Some(1.0), None, Some(20.0)])
            .max(formula(
                2,
                vec![Some(5.0), Some(15.0), Some(3.0), Some(25.0)],
            ))
            .unwrap()
            .max(formula(
                3,
                vec![Some(8.0), Some(21.0), Some(4.0), Some(18.0)],
            ))
            .unwrap();

        assert_eq!(composed.to_string(), "MAX(#1, #2, #3)");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![Some(10.0), Some(21.0), None, Some(25.0)]
        );
    }

    #[tokio::test]
    async fn test_avg() {
        let composed = formula(1, vec![Some(10.0), Some(20.0), None, Some(30.0), None])
            .avg(vec![
                formula(2, vec![Some(20.0), Some(30.0), Some(40.0), None, None]),
                formula(3, vec![Some(30.0), Some(40.0), Some(50.0), None, None]),
            ])
            .unwrap();

        assert_eq!(composed.to_string(), "AVG(#1, #2, #3)");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![Some(20.0), Some(30.0), Some(45.0), Some(30.0), None]
        );
    }

    #[tokio::test]
    async fn test_mixed_operations() {
        let composed = (formula(1, vec![Some(10.0), Some(20.0), None, Some(30.0)])
            + formula(2, vec![Some(5.0), Some(15.0), Some(25.0), Some(35.0)]))
        .max(formula(
            3,
            vec![Some(12.0), Some(48.0), Some(22.0), Some(40.0)],
        ))
        .unwrap()
            / 2.0;

        assert_eq!(composed.to_string(), "(MAX((#1 + #2), #3) / 2)");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![Some(7.5), Some(24.0), None, Some(32.5)]
        );

        let composed = formula(1, vec![Some(100.0), Some(200.0), None, Some(300.0)])
            .coalesce(formula(2, vec![Some(50.0), None, Some(25.0), Some(150.0)]))
            .unwrap()
            .min(formula(
                3,
                vec![Some(80.0), Some(180.0), Some(30.0), Some(250.0)],
            ))
            .unwrap();
        assert_eq!(composed.to_string(), "MIN(COALESCE(#1, #2), #3)");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![Some(80.0), Some(180.0), Some(25.0), Some(250.0)]
        );

        let composed = formula(1, vec![Some(10.0), Some(20.0), None, Some(30.0)])
            + formula(2, vec![Some(5.0), Some(15.0), Some(25.0), Some(35.0)])
            - formula(3, vec![Some(3.0), Some(13.0), Some(23.0), Some(33.0)]) * 2.0;
        assert_eq!(composed.to_string(), "((#1 + #2) - (#3 * 2))");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            vec![Some(9.0), Some(9.0), None, Some(-1.0)]
        );
    }

    #[tokio::test]
    async fn test_out_of_sync_handling() {
        let composed = formula(1, vec![Some(10.0), Some(20.0), Some(30.0), Some(40.0)])
            + formula_out_of_sync(2, vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);

        assert_eq!(composed.to_string(), "(#1 + #2)");
        assert_eq!(
            collect_values(&composed, Power::as_watts).await,
            // skips first message of receiver with older data.
            vec![Some(21.0), Some(32.0), Some(43.0)]
        );
    }

    #[tokio::test]
    async fn test_never_sync_handling() {
        let composed = formula(1, vec![Some(10.0), Some(20.0), Some(30.0), Some(40.0)])
            + formula_never_sync(2, vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);

        assert_eq!(composed.to_string(), "(#1 + #2)");
        let (collected_values, logs) =
            capture_logs(|| collect_values(&composed, Power::as_watts)).await;
        assert_eq!(
            collected_values,
            // sends nothing because the receivers could not be synchronized.
            vec![]
        );
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0],
            concat!(
                "ERROR frequenz_microgrid::logical_meter::formula::async_formula: ",
                "Couldn't synchronize receivers: ConnectionFailure: Could not receive sample: ",
                "channel closed.  Stopping processing."
            )
        );
    }

    fn formula(comp_id: u64, values: Vec<Option<f32>>) -> Formula<Power> {
        Formula::<Power>::Subscriber(Box::new(MockFormulaSubscriber::new(
            DateTime::<Utc>::UNIX_EPOCH,
            format!("#{comp_id}"),
            values,
        )))
    }

    fn formula_out_of_sync(comp_id: u64, values: Vec<Option<f32>>) -> Formula<Power, Power, f32> {
        Formula::<Power, Power, f32>::Subscriber(Box::new(MockFormulaSubscriber::new(
            DateTime::<Utc>::UNIX_EPOCH + TimeDelta::milliseconds(200),
            format!("#{comp_id}"),
            values,
        )))
    }

    fn formula_never_sync(comp_id: u64, values: Vec<Option<f32>>) -> Formula<Power, Power, f32> {
        Formula::<Power, Power, f32>::Subscriber(Box::new(MockFormulaSubscriber::new(
            DateTime::<Utc>::UNIX_EPOCH + TimeDelta::milliseconds(100),
            format!("#{comp_id}"),
            values,
        )))
    }

    async fn collect_values<T, F>(formula: &Formula<T>, extractor: F) -> Vec<Option<f32>>
    where
        T: Quantity,
        F: Fn(&T) -> f32,
    {
        let mut rx = formula.subscribe().await.unwrap();
        let mut values = Vec::new();
        while let Ok(sample) = rx.recv().await {
            values.push(sample.value().map(|v| extractor(&v)));
        }
        values
    }

    #[derive(Clone)]
    struct MockFormulaSubscriber {
        start_time: chrono::DateTime<Utc>,
        formula: String,
        values: Vec<Option<f32>>,
    }

    impl MockFormulaSubscriber {
        fn new(
            start_time: chrono::DateTime<Utc>,
            formula: String,
            values: Vec<Option<f32>>,
        ) -> Self {
            Self {
                start_time,
                formula,
                values,
            }
        }
    }

    impl std::fmt::Display for MockFormulaSubscriber {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.formula)
        }
    }

    #[async_trait]
    impl FormulaSubscriber for MockFormulaSubscriber {
        type QuantityType = Power;

        async fn subscribe(
            &self,
        ) -> Result<broadcast::Receiver<Sample<Self::QuantityType>>, Error> {
            let (tx, rx) = broadcast::channel(10);
            for (idx, &value) in self.values.iter().enumerate() {
                let sample = Sample::new(
                    self.start_time + TimeDelta::new(0, (idx * 200_000_000) as u32).unwrap(),
                    value.map(Power::from_watts),
                );
                let _ = tx.send(sample);
            }
            Ok(rx)
        }
    }
}
