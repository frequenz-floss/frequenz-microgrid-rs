// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Functions for applying formula operations on logical meter values.

use crate::{Sample, logical_meter::formula::async_formula::FormulaValue, quantity::Quantity};

pub(super) fn add_samples<Q: Quantity>(samples: &[FormulaValue<Q>]) -> Option<Sample<Q>> {
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

pub(super) fn subtract_samples<Q: Quantity>(samples: &[FormulaValue<Q>]) -> Option<Sample<Q>> {
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

pub(super) fn multiply_samples<QOut, QIn1, QIn2>(
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

pub(super) fn divide_samples<QOut, QDiv1, QDiv2>(
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
pub(super) fn coalesce_samples<Q: Quantity>(samples: &[FormulaValue<Q>]) -> Option<Sample<Q>> {
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

pub(super) fn min_samples<Q: Quantity>(samples: &[FormulaValue<Q>]) -> Option<Sample<Q>> {
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

pub(super) fn max_samples<Q: Quantity>(samples: &[FormulaValue<Q>]) -> Option<Sample<Q>> {
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

pub(super) fn avg_samples<Q: Quantity>(samples: &[FormulaValue<Q>]) -> Option<Sample<Q>> {
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
