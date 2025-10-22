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

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::*;

    #[test]
    fn test_add_samples() {
        use crate::quantity::Power;

        let ts = SystemTime::now().into();

        let sample1 = Sample::new(ts, Some(Power::from_watts(10.0_f32)));
        let sample2 = Sample::new(ts, Some(Power::from_watts(20.0_f32)));
        let sample3 = Sample::new(ts, Some(Power::from_watts(-8.0_f32)));

        let values = vec![
            FormulaValue::Sample(sample2),
            FormulaValue::Sample(sample3),
            FormulaValue::Quantity(Power::from_watts(100.0_f32)),
        ];
        let result = add_samples::<Power>(&values);
        assert_eq!(result.unwrap().value().unwrap().as_watts(), 112.0);

        let values = vec![
            FormulaValue::Sample(sample1),
            FormulaValue::Sample(sample2),
            FormulaValue::Sample(sample3),
        ];
        let result = add_samples::<Power>(&values);
        assert_eq!(result.unwrap().value().unwrap().as_watts(), 22.0);

        let sample_none = Sample::new(ts, None::<Power>);

        let values_with_none = vec![
            FormulaValue::Sample(sample1),
            FormulaValue::Sample(sample3),
            FormulaValue::Sample(sample_none),
        ];
        let result_with_none = add_samples::<Power>(&values_with_none);
        assert_eq!(result_with_none.unwrap().value(), None);
    }

    #[test]
    fn test_subtract_samples() {
        use crate::quantity::Current;

        let ts = SystemTime::now().into();

        let sample1 = Sample::new(ts, Some(Current::from_amperes(15.0_f32)));
        let sample2 = Sample::new(ts, Some(Current::from_amperes(5.0_f32)));
        let sample3 = Sample::new(ts, Some(Current::from_amperes(-3.0_f32)));

        let values = vec![
            FormulaValue::Sample(sample1),
            FormulaValue::Quantity(Current::from_amperes(-1.0_f32)),
            FormulaValue::Sample(sample2),
        ];
        let result = subtract_samples::<Current>(&values);
        assert_eq!(result.unwrap().value().unwrap().as_amperes(), 11.0);

        let values = vec![
            FormulaValue::Sample(sample1),
            FormulaValue::Sample(sample2),
            FormulaValue::Sample(sample3),
        ];
        let result = subtract_samples::<Current>(&values);
        assert_eq!(result.unwrap().value().unwrap().as_amperes(), 13.0);

        let sample_none = Sample::new(ts, None::<Current>);

        let values_with_none = vec![
            FormulaValue::Sample(sample1),
            FormulaValue::Sample(sample_none),
            FormulaValue::Sample(sample3),
        ];

        let result_with_none = subtract_samples::<Current>(&values_with_none);
        assert_eq!(result_with_none.unwrap().value(), None);
    }

    #[test]
    fn test_multiply_samples() {
        use crate::quantity::{Current, Voltage};

        let ts = SystemTime::now().into();

        let sample_voltage = Sample::new(ts, Some(Voltage::from_volts(10.0_f32)));
        let sample_current = Sample::new(ts, Some(Current::from_amperes(2.0_f32)));

        let result = multiply_samples(
            &FormulaValue::Sample(sample_voltage),
            &FormulaValue::Sample(sample_current),
        );

        assert_eq!(result.unwrap().value().unwrap().as_watts(), 20.0);

        let sample_none = Sample::new(ts, None::<Voltage>);

        let result_with_none = multiply_samples(
            &FormulaValue::Sample(sample_none),
            &FormulaValue::Sample(sample_current),
        );
        assert_eq!(result_with_none.unwrap().value(), None);

        let result_with_constant = multiply_samples(
            &FormulaValue::Quantity(Voltage::from_volts(10.0_f32)),
            &FormulaValue::Sample(sample_current),
        );
        assert_eq!(
            result_with_constant.unwrap().value().unwrap().as_watts(),
            20.0
        );

        let sample_f32 = Sample::new(ts, Some(3.0_f32));

        let result_f32 = multiply_samples(
            &FormulaValue::Quantity(Voltage::from_volts(5.0_f32)),
            &FormulaValue::Sample(sample_f32),
        );

        assert_eq!(result_f32.unwrap().value().unwrap().as_volts(), 15.0_f32);
    }

    #[test]
    fn test_divide_samples() {
        use crate::quantity::{Power, Voltage};

        let ts = SystemTime::now().into();

        let sample_power = Sample::new(ts, Some(Power::from_watts(100.0_f32)));
        let sample_voltage = Sample::new(ts, Some(Voltage::from_volts(10.0_f32)));
        let result = divide_samples(
            &FormulaValue::Sample(sample_power),
            &FormulaValue::Sample(sample_voltage),
        );
        assert_eq!(result.unwrap().value().unwrap().as_amperes(), 10.0);

        let sample_none = Sample::new(ts, None::<Power>);
        let result_with_none = divide_samples(
            &FormulaValue::Sample(sample_none),
            &FormulaValue::Sample(sample_voltage),
        );
        assert_eq!(result_with_none.unwrap().value(), None);

        let result_with_constant = divide_samples(
            &FormulaValue::Quantity(Power::from_watts(100.0_f32)),
            &FormulaValue::Sample(sample_voltage),
        );
        assert_eq!(
            result_with_constant.unwrap().value().unwrap().as_amperes(),
            10.0
        );

        let sample_f32 = Sample::new(ts, Some(2.0_f32));
        let result_f32 = divide_samples(
            &FormulaValue::Quantity(Voltage::from_volts(10.0_f32)),
            &FormulaValue::Sample(sample_f32),
        );
        assert_eq!(result_f32.unwrap().value().unwrap().as_volts(), 5.0_f32);
    }

    #[test]
    fn test_coalesce_samples() {
        use crate::quantity::Energy;

        let ts = SystemTime::now().into();

        let sample_none = Sample::new(ts, None::<Energy>);
        let sample1 = Sample::new(ts, Some(Energy::from_kilowatthours(5.0_f32)));
        let sample2 = Sample::new(ts, Some(Energy::from_kilowatthours(10.0_f32)));

        let values = vec![
            FormulaValue::Sample(sample_none),
            FormulaValue::Sample(sample1),
            FormulaValue::Sample(sample2),
        ];
        let result = coalesce_samples::<Energy>(&values);
        assert_eq!(result.unwrap().value().unwrap().as_kilowatthours(), 5.0);

        let values_all_none = vec![
            FormulaValue::Sample(sample_none),
            FormulaValue::Sample(sample_none),
        ];
        let result_all_none = coalesce_samples::<Energy>(&values_all_none);
        assert_eq!(result_all_none.unwrap().value(), None);

        let values_with_constant = vec![
            FormulaValue::Sample(sample_none),
            FormulaValue::Sample(sample_none),
            FormulaValue::Quantity(Energy::from_kilowatthours(8.0_f32)),
        ];
        let result_with_constant = coalesce_samples::<Energy>(&values_with_constant);
        assert_eq!(
            result_with_constant
                .unwrap()
                .value()
                .unwrap()
                .as_kilowatthours(),
            8.0
        );
    }

    #[test]
    fn test_min_samples() {
        use crate::quantity::Voltage;

        let ts = SystemTime::now().into();

        let sample1 = Sample::new(ts, Some(Voltage::from_volts(230.0_f32)));
        let sample2 = Sample::new(ts, Some(Voltage::from_volts(220.0_f32)));
        let sample3 = Sample::new(ts, Some(Voltage::from_volts(240.0_f32)));

        let values = vec![
            FormulaValue::Sample(sample1),
            FormulaValue::Sample(sample2),
            FormulaValue::Quantity(Voltage::from_volts(225.0_f32)),
            FormulaValue::Sample(sample3),
        ];

        let result = min_samples::<Voltage>(&values);
        assert_eq!(result.unwrap().value().unwrap().as_volts(), 220.0);

        let sample_none = Sample::new(ts, None::<Voltage>);

        let values_with_none = vec![
            FormulaValue::Sample(sample1),
            FormulaValue::Sample(sample_none),
            FormulaValue::Sample(sample3),
        ];

        let result_with_none = min_samples::<Voltage>(&values_with_none);
        assert_eq!(result_with_none.unwrap().value(), None);

        let values_with_constant = vec![
            FormulaValue::Sample(sample1),
            FormulaValue::Quantity(Voltage::from_volts(215.0_f32)),
            FormulaValue::Sample(sample3),
        ];

        let result_with_constant = min_samples::<Voltage>(&values_with_constant);
        assert_eq!(
            result_with_constant.unwrap().value().unwrap().as_volts(),
            215.0
        );
    }

    #[test]
    fn test_max_samples() {
        use crate::quantity::Voltage;

        let ts = SystemTime::now().into();

        let sample1 = Sample::new(ts, Some(Voltage::from_volts(230.0_f32)));
        let sample2 = Sample::new(ts, Some(Voltage::from_volts(220.0_f32)));
        let sample3 = Sample::new(ts, Some(Voltage::from_volts(240.0_f32)));

        let values = vec![
            FormulaValue::Sample(sample1),
            FormulaValue::Sample(sample2),
            FormulaValue::Quantity(Voltage::from_volts(225.0_f32)),
            FormulaValue::Sample(sample3),
        ];

        let result = max_samples::<Voltage>(&values);
        assert_eq!(result.unwrap().value().unwrap().as_volts(), 240.0_f32);

        let sample_none = Sample::new(ts, None::<Voltage>);

        let values_with_none = vec![
            FormulaValue::Sample(sample1),
            FormulaValue::Sample(sample_none),
            FormulaValue::Sample(sample3),
        ];
        let result_with_none = max_samples::<Voltage>(&values_with_none);
        assert_eq!(result_with_none.unwrap().value(), None);

        let values_with_constant = vec![
            FormulaValue::Sample(sample1),
            FormulaValue::Quantity(Voltage::from_volts(250.0_f32)),
            FormulaValue::Sample(sample3),
        ];
        let result_with_constant = max_samples::<Voltage>(&values_with_constant);
        assert_eq!(
            result_with_constant.unwrap().value().unwrap().as_volts(),
            250.0_f32
        );
    }
}
