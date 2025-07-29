// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! A trait specifying the output formula type and the corresponding PB metric,
//! for all metrics supported by the logical meter.

use super::formula;
use crate::proto::common::v1::metrics::Metric as MetricPb;

pub trait Metric: std::fmt::Display {
    type FormulaType: formula::Formula + formula::graph_formula_provider::GraphFormulaProvider;

    const METRIC: MetricPb;
}
