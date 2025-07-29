// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Metrics supported by the logical meter.

use crate::proto::common::v1::metrics::Metric as MetricPb;

use super::formula;

pub trait Metric: std::fmt::Display + std::fmt::Debug + Clone + Copy + PartialEq + Eq {
    type FormulaType: formula::Formula + formula::graph_formula_provider::GraphFormulaProvider;

    const METRIC: MetricPb;
}

macro_rules! define_metric {
    ($({name: $metric_name:ident, formula: $formula:ident}),+ $(,)?) => {
        $(
            // Define a metric
            #[derive(Debug, Clone, Copy, PartialEq, Eq)]
            pub struct $metric_name;

            // Implement the AcMetric trait for the metric
            impl Metric for $metric_name {
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

    {name: AcVoltage,             formula: CoalesceFormula},
    {name: AcVoltagePhase1N,      formula: CoalesceFormula},
    {name: AcVoltagePhase2N,      formula: CoalesceFormula},
    {name: AcVoltagePhase3N,      formula: CoalesceFormula},
    {name: AcVoltagePhase1Phase2, formula: CoalesceFormula},
    {name: AcVoltagePhase2Phase3, formula: CoalesceFormula},
    {name: AcVoltagePhase3Phase1, formula: CoalesceFormula},

    {name: AcFrequency, formula: CoalesceFormula},
}
