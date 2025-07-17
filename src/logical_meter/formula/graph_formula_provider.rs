// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! A composable formula type, that can be subscribed to.

use crate::Error;
use crate::logical_meter::logical_meter_actor;
use crate::proto::common::v1::microgrid::components::Component;
use crate::proto::common::v1::microgrid::components::ComponentConnection;
use frequenz_microgrid_component_graph::ComponentGraph;
use std::collections::BTreeSet;
use tokio::sync::mpsc;

use super::{AggregationFormula, CoalesceFormula};

macro_rules! graph_formula_provider {
    ($(($fnname:ident $(, $idsparam:ident)?)),+ $(,)?) => {$(

        fn $fnname<M: crate::metric::metric_trait::AcMetric>(
            _graph: &ComponentGraph<Component, ComponentConnection>,
            _metric: M,
            _instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
            $($idsparam: Option<BTreeSet<u64>>,)?
        ) -> Result<Self, Error> {
            return Err(Error::component_graph_error(
                format!(
                    "The component graph does not support {} formula generation for {}.",
                    stringify!($fnname),
                    _metric.to_string()
                )
            ));
        }

    )+};
}

/// Provides methods for generating corresponding formulas from the component
/// graph.
///
/// The component graph exposes methods to retrieve `AggregationFormula`s and
/// `CoalesceFormula`s for each of these metrics.  This trait provides a
/// way to generalize them.
pub trait GraphFormulaProvider: Sized {
    graph_formula_provider!(
        (grid),
        (consumer),
        (producer),
        (battery, _battery_ids),
        (chp, _chp_ids),
        (pv, _pv_inverter_ids),
        (ev_charger, _ev_charger_ids),
    );
}

macro_rules! impl_graph_formula_provider {
    ($(($fnname:ident, $graphfnname:ident$(, $idsparam:ident)?)),+ $(,)?) => {$(

        fn $fnname<M: crate::metric::metric_trait::AcMetric>(
            graph: &ComponentGraph<Component, ComponentConnection>,
            _metric: M,
            instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
            $($idsparam: Option<BTreeSet<u64>>,)?
        ) -> Result<Self, Error> {
            let formula = graph.$graphfnname($($idsparam)?).map_err(|e| {
                Error::component_graph_error(
                    format!("Could not get {} formula: {e}", stringify!($fnname))
                )
            })?;
            Ok(Self::new(formula, M::METRIC, instructions_tx))
        }
    )+};
}

impl GraphFormulaProvider for AggregationFormula {
    impl_graph_formula_provider!(
        (grid, grid_formula),
        (consumer, consumer_formula),
        (producer, producer_formula),
        (battery, battery_formula, battery_ids),
        (chp, chp_formula, chp_ids),
        (pv, pv_formula, pv_inverter_ids),
        (ev_charger, ev_charger_formula, ev_charger_ids),
    );
}

impl GraphFormulaProvider for CoalesceFormula {
    impl_graph_formula_provider!(
        (grid, grid_coalesce_formula),
        (battery, battery_ac_coalesce_formula, battery_ids),
        (pv, pv_ac_coalesce_formula, pv_inverter_ids),
    );
}
