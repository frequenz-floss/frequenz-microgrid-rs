// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! A nested formula that can contain other formulas.

use async_trait::async_trait;
use tokio::sync::broadcast::{self, error::RecvError};

use crate::{Error, Sample, logical_meter::formula::FormulaSubscriber, quantity::Quantity};

/// A composable Formula.
pub enum Formula<QOut, QIn1 = QOut, QIn2 = f32>
where
    QOut: Quantity + 'static,
    QIn1: Quantity + 'static,
    QIn2: Quantity + 'static,
    QIn1: std::ops::Mul<QIn2, Output = QOut>,
{
    Subscriber(Box<dyn FormulaSubscriber<QuantityType = QOut>>),
    Coalesce(Vec<FormulaOperand<QOut>>),
    Min(Vec<FormulaOperand<QOut>>),
    Max(Vec<FormulaOperand<QOut>>),
    Avg(Vec<FormulaOperand<QOut>>),
    Add(Vec<FormulaOperand<QOut>>),
    Subtract(Vec<FormulaOperand<QOut>>),
    Multiply(FormulaOperand<QIn1>, FormulaOperand<QIn2>),
    Divide(FormulaOperand<QIn1>, FormulaOperand<QIn2>),
}

pub enum FormulaOperand<Q: Quantity + 'static> {
    Formula(Box<dyn FormulaSubscriber<QuantityType = Q>>),
    Stream(broadcast::Receiver<crate::Sample<Q>>, String),
    Quantity(Q),
}

impl<Q: Quantity + 'static> FormulaOperand<Q> {
    async fn subscribe(&self) -> Result<FormulaOperand<Q>, Error> {
        match self {
            FormulaOperand::Formula(formula_subscriber) => Ok(FormulaOperand::Stream(
                (*formula_subscriber).subscribe().await?,
                formula_subscriber.to_string(),
            )),
            FormulaOperand::Stream(receiver, name) => {
                Ok(FormulaOperand::Stream(receiver.resubscribe(), name.clone()))
            }
            FormulaOperand::Quantity(quantity) => Ok(FormulaOperand::Quantity(*quantity)),
        }
    }

    async fn recv(&mut self) -> Result<FormulaValue<Q>, RecvError> {
        match self {
            FormulaOperand::Formula(..) => {
                tracing::error!("Internal: FormulaItem::recv called on unsubscribed FormulaItem.");
                Err(RecvError::Closed)
            }
            FormulaOperand::Stream(receiver, _) => match receiver.recv().await {
                Ok(sample) => Ok(FormulaValue::Sample(sample)),
                Err(e) => Err(e),
            },
            FormulaOperand::Quantity(q) => Ok(FormulaValue::Quantity(*q)),
        }
    }
}

impl<QOut, QIn1, QIn2> From<Formula<QOut, QIn1, QIn2>> for FormulaOperand<QOut>
where
    QOut: Quantity + 'static,
    QIn1: Quantity + 'static,
    QIn2: Quantity + 'static,
    QOut: std::ops::Div<QIn2, Output = QIn1>,
    QIn1: std::ops::Mul<QIn2, Output = QOut> + std::ops::Div<QIn2, Output = QOut>,
{
    fn from(formula: Formula<QOut, QIn1, QIn2>) -> Self {
        FormulaOperand::Formula(Box::new(formula))
    }
}

impl<Q> From<(broadcast::Receiver<crate::Sample<Q>>, String)> for FormulaOperand<Q>
where
    Q: Quantity + 'static,
{
    fn from(value: (broadcast::Receiver<crate::Sample<Q>>, String)) -> Self {
        FormulaOperand::Stream(value.0, value.1)
    }
}

impl<Q> From<Q> for FormulaOperand<Q>
where
    Q: Quantity + 'static,
{
    fn from(quantity: Q) -> Self {
        FormulaOperand::Quantity(quantity)
    }
}

impl<Q: Quantity + std::fmt::Display> std::fmt::Display for FormulaOperand<Q> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FormulaOperand::Formula(formula) => write!(f, "{formula}"),
            FormulaOperand::Stream(_, name) => write!(f, "{name}"),
            FormulaOperand::Quantity(q) => write!(f, "{q}"),
        }
    }
}

#[derive(Debug)]
enum FormulaValue<Q: Quantity> {
    Sample(crate::Sample<Q>),
    Quantity(Q),
}

fn format_exprs<Q: Quantity + std::fmt::Display>(
    f: &mut std::fmt::Formatter<'_>,
    exprs: &[FormulaOperand<Q>],
    prefix: &str,
    sep: &str,
) -> std::fmt::Result {
    write!(f, "{prefix}(")?;
    for (i, expr) in exprs.iter().enumerate() {
        if i > 0 {
            write!(f, "{sep}")?;
        }
        write!(f, "{expr}")?;
    }
    write!(f, ")")
}

impl<QOut, QIn1, QIn2> std::fmt::Display for Formula<QOut, QIn1, QIn2>
where
    QOut: Quantity,
    QIn1: Quantity,
    QIn2: Quantity,
    QIn1: std::ops::Mul<QIn2, Output = QOut>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Formula::Subscriber(formula) => formula.fmt(f),
            Formula::Coalesce(exprs) => format_exprs(f, exprs, "COALESCE", ", "),
            Formula::Min(exprs) => format_exprs(f, exprs, "MIN", ", "),
            Formula::Max(exprs) => format_exprs(f, exprs, "MAX", ", "),
            Formula::Avg(exprs) => format_exprs(f, exprs, "AVG", ", "),
            Formula::Add(exprs) => format_exprs(f, exprs, "", " + "),
            Formula::Subtract(exprs) => format_exprs(f, exprs, "", " - "),
            Formula::Multiply(lhs, rhs) => write!(f, "({lhs} * {rhs})"),
            Formula::Divide(lhs, rhs) => write!(f, "({lhs} / {rhs})"),
        }
    }
}

async fn synchronize_receivers<Q: Quantity>(
    formula_items: &mut [FormulaOperand<Q>],
) -> Result<Vec<FormulaValue<Q>>, crate::Error> {
    let mut latest = vec![];
    for item in formula_items.iter_mut() {
        match item.recv().await {
            Ok(vv) => latest.push(vv),
            Err(_) => todo!(),
        };
    }

    let max_ts = latest
        .iter()
        .filter_map(|value| match value {
            FormulaValue::Sample(sample) => Some(sample.timestamp()),
            FormulaValue::Quantity(_) => None,
        })
        .max()
        .ok_or_else(|| crate::Error::internal("No receivers to synchronize".to_string()))?;

    // synchronize all receivers to the latest timestamp
    for (ii, item) in formula_items.iter_mut().enumerate() {
        let FormulaOperand::Stream(receiver, _) = item else {
            continue;
        };
        let mut ctr = 0;
        while let FormulaValue::Sample(sample) = latest[ii]
            && sample.timestamp() != max_ts
            && ctr < 10
        {
            ctr += 1;
            match receiver.recv().await {
                Ok(sample) => latest[ii] = FormulaValue::Sample(sample),
                Err(e) => {
                    return Err(crate::Error::connection_failure(format!(
                        "Could not receive sample: {e}"
                    )));
                }
            };
        }

        if let FormulaValue::Sample(sample) = latest[ii]
            && sample.timestamp() != max_ts
        {
            return Err(crate::Error::internal(format!(
                "Could not synchronize receiver {} to the latest timestamp: {}",
                ii, max_ts
            )));
        }
    }

    Ok(latest)
}

async fn synchronize_two_receivers<Q1: Quantity, Q2: Quantity>(
    formula_item1: &mut FormulaOperand<Q1>,
    formula_item2: &mut FormulaOperand<Q2>,
) -> Result<(FormulaValue<Q1>, FormulaValue<Q2>), crate::Error> {
    match (formula_item1, formula_item2) {
        (FormulaOperand::Stream(rx1, _), FormulaOperand::Stream(rx2, _)) => {
            let mut latest1 = rx1.recv().await.map_err(|e| {
                crate::Error::connection_failure(format!("Could not receive sample: {e}"))
            })?;
            let mut latest2 = rx2.recv().await.map_err(|e| {
                crate::Error::connection_failure(format!("Could not receive sample: {e}"))
            })?;

            let max_ts = latest1.timestamp().max(latest2.timestamp());

            let mut ctr = 0;
            while latest1.timestamp() != max_ts && ctr < 10 {
                ctr += 1;
                latest1 = rx1.recv().await.map_err(|e| {
                    crate::Error::connection_failure(format!("Could not receive sample: {e}"))
                })?;
            }
            if latest1.timestamp() != max_ts {
                return Err(crate::Error::internal(format!(
                    "Could not synchronize receiver 1 to the latest timestamp: {}",
                    max_ts
                )));
            }

            ctr = 0;
            while latest2.timestamp() != max_ts && ctr < 10 {
                ctr += 1;
                latest2 = rx2.recv().await.map_err(|e| {
                    crate::Error::connection_failure(format!("Could not receive sample: {e}"))
                })?;
            }
            if latest2.timestamp() != max_ts {
                return Err(crate::Error::internal(format!(
                    "Could not synchronize receiver 2 to the latest timestamp: {}",
                    max_ts
                )));
            }
            Ok((FormulaValue::Sample(latest1), FormulaValue::Sample(latest2)))
        }
        (FormulaOperand::Formula(..), _) | (_, FormulaOperand::Formula(..)) => {
            Err(crate::Error::internal(
                "Internal: synchronize_two_receivers called on unsubscribed FormulaItem."
                    .to_string(),
            ))
        }
        (item1, item2) => Ok((
            match item1.recv().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("Could not receive sample: {e}");
                    return Err(crate::Error::connection_failure(format!(
                        "Could not receive sample: {e}"
                    )));
                }
            },
            match item2.recv().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("Could not receive sample: {e}");
                    return Err(crate::Error::connection_failure(format!(
                        "Could not receive sample: {e}"
                    )));
                }
            },
        )),
    }
}

async fn run_formula<Q: Quantity>(
    mut formula_items: Vec<FormulaOperand<Q>>,
    result_sender: broadcast::Sender<crate::Sample<Q>>,
    apply_fn: fn(&[FormulaValue<Q>]) -> Option<crate::Sample<Q>>,
) {
    match synchronize_receivers(&mut formula_items).await {
        Ok(latest) => {
            let value = match apply_fn(&latest) {
                Some(sample) => sample,
                None => {
                    tracing::debug!(
                        "No value computed. Stopping processing. Input values: {:?}",
                        latest
                    );
                    return;
                }
            };
            if let Err(err) = result_sender.send(value) {
                tracing::debug!("No subscribers: {}. Stopping processing", err);
                return;
            }
        }
        Err(e) => {
            tracing::error!(
                "Couldn't synchronize receivers: {}.  Stopping processing.",
                e
            );
            return;
        }
    };
    loop {
        let mut latest = vec![];
        for formula_item in formula_items.iter_mut() {
            latest.push(match formula_item.recv().await {
                Ok(value) => value,
                Err(RecvError::Closed) => {
                    tracing::debug!("input receiver closed. stopping formula processing.");
                    return;
                }
                Err(RecvError::Lagged(count)) => {
                    tracing::warn!("input receiver lagged by {count} samples.");
                    continue;
                }
            });
        }
        if latest.is_empty() {
            tracing::debug!("No active input receivers.  Stopping processing.");
            return;
        }

        let value = match apply_fn(&latest) {
            Some(sample) => sample,
            None => {
                tracing::debug!(
                    "No value computed. Stopping processing. Input values: {:?}",
                    latest
                );
                return;
            }
        };
        if let Err(err) = result_sender.send(value) {
            tracing::debug!("No subscribers: {}. Stopping processing", err);
            return;
        }
    }
}

async fn run_two_item_formula<QOut, QIn1, QIn2>(
    mut formula_item1: FormulaOperand<QIn1>,
    mut formula_item2: FormulaOperand<QIn2>,
    result_sender: broadcast::Sender<crate::Sample<QOut>>,
    apply_fn: fn(&FormulaValue<QIn1>, &FormulaValue<QIn2>) -> Option<crate::Sample<QOut>>,
) where
    QOut: Quantity,
    QIn1: Quantity,
    QIn2: Quantity,
{
    match synchronize_two_receivers(&mut formula_item1, &mut formula_item2).await {
        Ok((value1, value2)) => {
            let value = match apply_fn(&value1, &value2) {
                Some(sample) => sample,
                None => {
                    tracing::debug!(
                        "No value computed. Stopping processing. Input values: {:?}, {:?}",
                        value1,
                        value2
                    );
                    return;
                }
            };
            if let Err(err) = result_sender.send(value) {
                tracing::debug!("No subscribers: {}. Stopping processing", err);
                return;
            }
        }
        Err(e) => {
            tracing::error!(
                "Couldn't synchronize receivers: {}.  Stopping processing.",
                e
            );
            return;
        }
    }

    loop {
        let (value1, value2) = match (formula_item1.recv().await, formula_item2.recv().await) {
            (Ok(v1), Ok(v2)) => (v1, v2),
            (Err(RecvError::Closed), _) | (_, Err(RecvError::Closed)) => {
                tracing::debug!("input receiver closed. stopping formula processing.");
                return;
            }
            (Err(RecvError::Lagged(count)), _) | (_, Err(RecvError::Lagged(count))) => {
                tracing::warn!("input receiver lagged by {count} samples.");
                continue;
            }
        };

        let result = match apply_fn(&value1, &value2) {
            Some(sample) => sample,
            None => {
                tracing::error!(
                    "No value computed. Stopping processing. Input values: {:?}, {:?}",
                    value1,
                    value2
                );
                return;
            }
        };

        if let Err(err) = result_sender.send(result) {
            tracing::debug!("No subscribers: {}. Stopping processing", err);
            return;
        }
    }
}

fn multiply_samples<QOut, QIn1, QIn2>(
    value1: &FormulaValue<QIn1>,
    value2: &FormulaValue<QIn2>,
) -> Option<Sample<QOut>>
where
    QOut: Quantity,
    QIn1: Quantity,
    QIn2: Quantity,
    QIn1: std::ops::Mul<QIn2, Output = QOut>,
{
    let mut ts = None;

    let v1 = match value1 {
        FormulaValue::Sample(sample) => {
            ts = Some(sample.timestamp());
            if let Some(value) = sample.value() {
                value
            } else {
                return Some(Sample::new(sample.timestamp(), None));
            }
        }
        FormulaValue::Quantity(q) => *q,
    };

    let v2 = match value2 {
        FormulaValue::Sample(sample) => {
            ts = Some(sample.timestamp());
            if let Some(value) = sample.value() {
                value
            } else {
                return Some(Sample::new(sample.timestamp(), None));
            }
        }
        FormulaValue::Quantity(q) => *q,
    };

    ts.map(|ts| Sample::new(ts, Some(v1 * v2)))
}

fn divide_samples<QOut, QDiv1, QDiv2>(
    value1: &FormulaValue<QDiv1>,
    value2: &FormulaValue<QDiv2>,
) -> Option<Sample<QOut>>
where
    QOut: Quantity,
    QDiv1: Quantity,
    QDiv2: Quantity,
    QDiv1: std::ops::Div<QDiv2, Output = QOut>,
{
    let mut ts = None;

    let v1 = match value1 {
        FormulaValue::Sample(sample) => {
            ts = Some(sample.timestamp());
            if let Some(value) = sample.value() {
                value
            } else {
                return Some(Sample::new(sample.timestamp(), None));
            }
        }
        FormulaValue::Quantity(q) => *q,
    };

    let v2 = match value2 {
        FormulaValue::Sample(sample) => {
            ts = Some(sample.timestamp());
            if let Some(value) = sample.value() {
                value
            } else {
                return Some(Sample::new(sample.timestamp(), None));
            }
        }
        FormulaValue::Quantity(q) => *q,
    };

    ts.map(|ts| {
        Sample::new(
            ts,
            if v2 != QDiv2::zero() {
                Some(v1 / v2)
            } else {
                None
            },
        )
    })
}

// TODO: handle None values more strictly.
fn coalesce_samples<Q: Quantity>(samples: &[FormulaValue<Q>]) -> Option<Sample<Q>> {
    let mut ts = None;
    for value in samples {
        match value {
            FormulaValue::Sample(sample) => {
                ts = Some(sample.timestamp());
                if sample.value().is_some() {
                    return Some(*sample);
                }
            }
            FormulaValue::Quantity(q) => {
                return ts.map(|ts| Sample::new(ts, Some(*q)));
            }
        }
    }

    ts.map(|ts| Sample::new(ts, None))
}

fn min_samples<Q: Quantity>(samples: &[FormulaValue<Q>]) -> Option<Sample<Q>> {
    let mut min: Option<Q> = None;
    let mut ts = None;
    for value in samples {
        match value {
            FormulaValue::Sample(sample) => {
                ts = Some(sample.timestamp());

                match sample.value() {
                    Some(v) => {
                        min = Some(match min {
                            Some(current_min) => {
                                if v < current_min {
                                    v
                                } else {
                                    current_min
                                }
                            }
                            None => v,
                        });
                    }
                    None => return Some(*sample),
                }
            }
            FormulaValue::Quantity(q) => {
                min = Some(match min {
                    Some(current_min) => {
                        if *q < current_min {
                            *q
                        } else {
                            current_min
                        }
                    }
                    None => *q,
                });
            }
        }
    }
    ts.map(|ts| Sample::new(ts, min))
}

fn max_samples<Q: Quantity>(samples: &[FormulaValue<Q>]) -> Option<Sample<Q>> {
    let mut min: Option<Q> = None;
    let mut ts = None;
    for value in samples {
        match value {
            FormulaValue::Sample(sample) => {
                ts = Some(sample.timestamp());

                match sample.value() {
                    Some(v) => {
                        min = Some(match min {
                            Some(current_min) => {
                                if v > current_min {
                                    v
                                } else {
                                    current_min
                                }
                            }
                            None => v,
                        });
                    }
                    None => return Some(*sample),
                }
            }
            FormulaValue::Quantity(q) => {
                min = Some(match min {
                    Some(current_min) => {
                        if *q > current_min {
                            *q
                        } else {
                            current_min
                        }
                    }
                    None => *q,
                });
            }
        }
    }
    ts.map(|ts| Sample::new(ts, min))
}

fn avg_samples<Q: Quantity>(samples: &[FormulaValue<Q>]) -> Option<Sample<Q>> {
    let mut sum = Q::default();
    let mut count = 0;
    let mut ts = None;
    for value in samples {
        match value {
            FormulaValue::Sample(sample) => {
                ts = Some(sample.timestamp());
                if let Some(v) = sample.value() {
                    sum = sum + v;
                    count += 1;
                }
            }
            FormulaValue::Quantity(q) => {
                sum = sum + *q;
                count += 1;
            }
        }
    }
    ts.map(|ts| Sample::new(ts, (count > 0).then(|| sum / count as f32)))
}

fn add_samples<Q: Quantity>(samples: &[FormulaValue<Q>]) -> Option<Sample<Q>> {
    let mut sum = Q::default();
    let mut ts = None;
    for value in samples {
        match value {
            FormulaValue::Sample(sample) => {
                ts = Some(sample.timestamp());
                if let Some(v) = sample.value() {
                    sum = sum + v;
                } else {
                    return Some(*sample);
                }
            }
            FormulaValue::Quantity(q) => {
                sum = sum + *q;
            }
        }
    }
    ts.map(|ts| Sample::new(ts, Some(sum)))
}

fn subtract_samples<Q: Quantity>(samples: &[FormulaValue<Q>]) -> Option<Sample<Q>> {
    let mut ts = None;

    let mut iter = samples.iter();
    let first = iter.next()?;

    let mut result = match first {
        FormulaValue::Sample(sample) => {
            ts = Some(sample.timestamp());
            if let Some(v) = sample.value() {
                v
            } else {
                return Some(*sample);
            }
        }
        FormulaValue::Quantity(q) => *q,
    };

    for value in iter {
        match value {
            FormulaValue::Sample(sample) => {
                ts = Some(sample.timestamp());
                if let Some(v) = sample.value() {
                    result = result - v;
                } else {
                    return Some(*sample);
                }
            }
            FormulaValue::Quantity(q) => {
                result = result - *q;
            }
        }
    }
    ts.map(|ts| Sample::new(ts, Some(result)))
}

#[async_trait]
impl<QOut, QIn1, QIn2> FormulaSubscriber for Formula<QOut, QIn1, QIn2>
where
    QOut: Quantity + 'static,
    QIn1: Quantity + 'static + std::ops::Mul<QIn2, Output = QOut>,
    QIn2: Quantity + 'static,
    // The below works only as long as QIn2 is unit-less.  If we want to
    // support division for other quantities, we need to introduce additional
    // generic types for division.
    QIn1: std::ops::Div<QIn2, Output = QOut>,
{
    type QuantityType = QOut;

    async fn subscribe(&self) -> Result<broadcast::Receiver<Sample<QOut>>, Error> {
        match &self {
            Formula::Subscriber(formula) => (*formula).subscribe().await,
            Formula::Coalesce(exprs)
            | Formula::Min(exprs)
            | Formula::Max(exprs)
            | Formula::Avg(exprs)
            | Formula::Add(exprs)
            | Formula::Subtract(exprs) => {
                let mut formula_items = Vec::new();
                for expr in exprs {
                    formula_items.push(expr.subscribe().await?);
                }

                let (tx, rx) = broadcast::channel(100);
                tokio::spawn(run_formula(
                    formula_items,
                    tx,
                    match self {
                        Formula::Coalesce(_) => coalesce_samples,
                        Formula::Min(_) => min_samples,
                        Formula::Max(_) => max_samples,
                        Formula::Avg(_) => avg_samples,
                        Formula::Add(_) => add_samples,
                        Formula::Subtract(_) => subtract_samples,
                        _ => unreachable!(),
                    },
                ));
                Ok(rx)
            }
            Formula::Multiply(lhs, rhs) | Formula::Divide(lhs, rhs) => {
                let lhs = lhs.subscribe().await?;
                let rhs = rhs.subscribe().await?;

                let (tx, rx) = broadcast::channel(100);
                tokio::spawn(run_two_item_formula(
                    lhs,
                    rhs,
                    tx,
                    match self {
                        Formula::Multiply(_, _) => multiply_samples,
                        Formula::Divide(_, _) => divide_samples,
                        _ => unreachable!(),
                    },
                ));

                Ok(rx)
            }
        }
    }
}
