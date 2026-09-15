// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Metrics supported by the logical meter.

use crate::client::proto::common::metrics::Metric as MetricPb;

/// Which family of component-graph formula generators a metric uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FormulaKind {
    /// Sums and differences over components, for power and current.
    Aggregation,
    /// The first available source, for voltage and frequency.
    Coalesce,
}

/// A metric the logical meter can stream, with its quantity type and the
/// kind of graph formula that computes it.
pub trait Metric:
    std::fmt::Display + std::fmt::Debug + Clone + Copy + PartialEq + Eq + Sync + 'static
{
    /// The quantity streamed for this metric.
    type QuantityType: crate::quantity::Quantity;

    /// The protobuf metric read from components.
    const METRIC: MetricPb;

    /// The graph formula family used to compute this metric.
    const KIND: FormulaKind;

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
                const KIND: FormulaKind = FormulaKind::$kind;

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
    { name: DcPower,               kind: Aggregation, quantity: Power },
    { name: AcPowerActive,         kind: Aggregation, quantity: Power },
    { name: AcPowerReactive,       kind: Aggregation, quantity: ReactivePower },
    { name: AcPowerApparent,       kind: Aggregation, quantity: ApparentPower },
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

#[cfg(test)]
mod tests {
    use super::{AcPowerApparent, FormulaKind, Metric, MetricPb};

    #[test]
    fn test_ac_power_apparent_metric() {
        assert_eq!(AcPowerApparent::METRIC, MetricPb::AcPowerApparent);
        assert_eq!(AcPowerApparent::KIND, FormulaKind::Aggregation);
        assert_eq!(AcPowerApparent::str_name(), "AcPowerApparent");
    }
}
