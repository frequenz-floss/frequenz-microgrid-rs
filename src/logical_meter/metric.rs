// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Metrics supported by the logical meter.

pub(crate) mod metric_trait;

use crate::proto::common::v1::metrics::Metric as MetricPb;

use super::formula;
use metric_trait::AcMetric;

macro_rules! define_metric {
    ($({name: $metric_name:ident, formula: $formula:ident}),+ $(,)?) => {
        $(
            // Define a metric
            #[derive(Debug, Clone, Copy, PartialEq, Eq)]
            pub struct $metric_name;

            // Implement the AcMetric trait for the metric
            impl AcMetric for $metric_name {
                type FormulaType = formula::$formula;

                const METRIC: MetricPb = MetricPb::$metric_name;
            }

            impl std::fmt::Display for $metric_name {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}", stringify!($metric_name))
                }
            }

        )+
    };
}

define_metric! {
    {name: AcActivePower,   formula: AggregationFormula},
    {name: AcReactivePower, formula: AggregationFormula},
    {name: AcCurrent,       formula: AggregationFormula},
    {name: AcCurrentPhase1, formula: AggregationFormula},
    {name: AcCurrentPhase2, formula: AggregationFormula},
    {name: AcCurrentPhase3, formula: AggregationFormula},
}

/// Metrics supported by the logical meter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    // AC Power
    AcActivePower = MetricPb::AcActivePower as isize,
    AcReactivePower = MetricPb::AcReactivePower as isize,
    // AC Current
    AcCurrent = MetricPb::AcCurrent as isize,
    AcCurrentPhase1 = MetricPb::AcCurrentPhase1 as isize,
    AcCurrentPhase2 = MetricPb::AcCurrentPhase2 as isize,
    AcCurrentPhase3 = MetricPb::AcCurrentPhase3 as isize,
    // AC Voltage
    AcVoltage = MetricPb::AcVoltage as isize,
    AcVoltagePhase1N = MetricPb::AcVoltagePhase1N as isize,
    AcVoltagePhase2N = MetricPb::AcVoltagePhase2N as isize,
    AcVoltagePhase3N = MetricPb::AcVoltagePhase3N as isize,
    AcVoltagePhase1Phase2 = MetricPb::AcVoltagePhase1Phase2 as isize,
    AcVoltagePhase2Phase3 = MetricPb::AcVoltagePhase2Phase3 as isize,
    AcVoltagePhase3Phase1 = MetricPb::AcVoltagePhase3Phase1 as isize,
    // AC Frequency
    AcFrequency = MetricPb::AcFrequency as isize,
}

impl Metric {
    pub(super) fn power(self) -> bool {
        matches!(self, Metric::AcActivePower | Metric::AcReactivePower)
    }
    pub(super) fn current(self) -> bool {
        matches!(
            self,
            Metric::AcCurrent
                | Metric::AcCurrentPhase1
                | Metric::AcCurrentPhase2
                | Metric::AcCurrentPhase3
        )
    }
}
