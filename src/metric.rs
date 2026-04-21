// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Metrics supported by the logical meter.

use crate::logical_meter::formula::aggregation_formula::AggregationFormula;
use crate::logical_meter::formula::coalesce_formula::CoalesceFormula;
use crate::{
    logical_meter::formula, logical_meter::formula::FormulaSubscriber,
    proto::common::metrics::Metric as MetricPb,
};

pub trait Metric:
    std::fmt::Display + std::fmt::Debug + Clone + Copy + PartialEq + Eq + Sync + 'static
{
    type FormulaType: FormulaSubscriber<QuantityType = Self::QuantityType>
        + formula::graph_formula_provider::GraphFormulaProvider<MetricType = Self>
        + 'static;

    type QuantityType: crate::quantity::Quantity;

    const METRIC: MetricPb;

    fn str_name() -> &'static str;
}

macro_rules! define_metric {
    ($({
        name: $metric_name:ident,
        formula: $formula:ident,
        quantity: $quantity:ident
    }),+ $(,)?) => {
        $(
            // Define a metric
            #[derive(Debug, Clone, Copy, PartialEq, Eq)]
            pub struct $metric_name;

            // Implement the AcMetric trait for the metric
            impl Metric for $metric_name {
                type FormulaType = $formula<$metric_name>;
                type QuantityType = crate::quantity::$quantity;

                const METRIC: MetricPb = MetricPb::$metric_name;

                fn str_name() -> &'static str {
                    stringify!($metric_name)
                }
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
    { name: AcPowerActive,         formula: AggregationFormula, quantity: Power },
    { name: AcPowerReactive,       formula: AggregationFormula, quantity: ReactivePower },
    { name: AcCurrent,             formula: AggregationFormula, quantity: Current },
    { name: AcCurrentPhase1,       formula: AggregationFormula, quantity: Current },
    { name: AcCurrentPhase2,       formula: AggregationFormula, quantity: Current },
    { name: AcCurrentPhase3,       formula: AggregationFormula, quantity: Current },

    { name: AcVoltage,             formula: CoalesceFormula,    quantity: Voltage },
    { name: AcVoltagePhase1N,      formula: CoalesceFormula,    quantity: Voltage },
    { name: AcVoltagePhase2N,      formula: CoalesceFormula,    quantity: Voltage },
    { name: AcVoltagePhase3N,      formula: CoalesceFormula,    quantity: Voltage },
    { name: AcVoltagePhase1Phase2, formula: CoalesceFormula,    quantity: Voltage },
    { name: AcVoltagePhase2Phase3, formula: CoalesceFormula,    quantity: Voltage },
    { name: AcVoltagePhase3Phase1, formula: CoalesceFormula,    quantity: Voltage },

    { name: AcFrequency,           formula: CoalesceFormula,    quantity: Frequency },
}
