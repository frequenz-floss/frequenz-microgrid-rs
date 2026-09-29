// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Metrics supported by the logical meter.

use crate::client::proto::common::metrics::Metric as MetricPb;

/// Which family of component-graph formula generators a metric uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FormulaKind {
    /// Aggregates over components, for power and current.
    Aggregation,
    /// The first available source, for voltage and frequency.
    Coalesce,
}

/// The kind of graph formula that computes a metric.
pub(crate) trait GraphFormulaKind {
    const FORMULA_KIND: FormulaKind;
}

/// A metric the logical meter can stream, with its quantity type.
#[expect(private_bounds)]
pub trait Metric:
    GraphFormulaKind
    + std::fmt::Display
    + std::fmt::Debug
    + Clone
    + Copy
    + PartialEq
    + Eq
    + Sync
    + 'static
{
    /// The quantity streamed for this metric.
    type QuantityType: crate::quantity::Quantity;

    /// The protobuf metric read from components.
    const METRIC: MetricPb;

    /// The metric's name, e.g. `AcPowerActive`.
    fn str_name() -> &'static str;
}

macro_rules! define_metric {
    ($({
        name: $metric_name:ident,
        kind: $kind:ident,
        quantity: $quantity:ident
    }),+ $(,)?) => {
        $(
            #[doc = concat!("The `", stringify!($metric_name), "` metric.")]
            #[derive(Debug, Clone, Copy, PartialEq, Eq)]
            pub struct $metric_name;

            impl Metric for $metric_name {
                type QuantityType = crate::quantity::$quantity;

                const METRIC: MetricPb = MetricPb::$metric_name;

                fn str_name() -> &'static str {
                    stringify!($metric_name)
                }
            }

            impl GraphFormulaKind for $metric_name {
                const FORMULA_KIND: FormulaKind = FormulaKind::$kind;
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
    { name: DcPower,               kind: Aggregation, quantity: Power },
    { name: AcPowerActive,         kind: Aggregation, quantity: Power },
    { name: AcPowerReactive,       kind: Aggregation, quantity: ReactivePower },
    { name: AcCurrent,             kind: Aggregation, quantity: Current },
    { name: AcCurrentPhase1,       kind: Aggregation, quantity: Current },
    { name: AcCurrentPhase2,       kind: Aggregation, quantity: Current },
    { name: AcCurrentPhase3,       kind: Aggregation, quantity: Current },

    { name: AcVoltage,             kind: Coalesce,    quantity: Voltage },
    { name: AcVoltagePhase1N,      kind: Coalesce,    quantity: Voltage },
    { name: AcVoltagePhase2N,      kind: Coalesce,    quantity: Voltage },
    { name: AcVoltagePhase3N,      kind: Coalesce,    quantity: Voltage },
    { name: AcVoltagePhase1Phase2, kind: Coalesce,    quantity: Voltage },
    { name: AcVoltagePhase2Phase3, kind: Coalesce,    quantity: Voltage },
    { name: AcVoltagePhase3Phase1, kind: Coalesce,    quantity: Voltage },

    { name: AcFrequency,           kind: Coalesce,    quantity: Frequency },
}
