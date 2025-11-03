// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! A composable formula type, that can be subscribed to.

use crate::Error;
use crate::logical_meter::formula::FormulaParams;
use crate::logical_meter::logical_meter_actor;
use crate::metric::Metric;
use crate::proto::common::v1alpha8::microgrid::electrical_components::{
    ElectricalComponent, ElectricalComponentConnection,
};

use frequenz_microgrid_component_graph::ComponentGraph;
use std::collections::BTreeSet;
use tokio::sync::mpsc;

use super::{AggregationFormula, CoalesceFormula};

macro_rules! graph_formula_provider {
    ($(($fnname:ident $(, ids:$idsparam:ident)? $(, id:$idparam:ident)?)),+ $(,)?) => {$(

        fn $fnname(
            _graph: &ComponentGraph<ElectricalComponent, ElectricalComponentConnection>,
            _metric: Self::MetricType,
            _instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
            $($idsparam: Option<BTreeSet<u64>>,)?
            $($idparam: u64,)?
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
    type MetricType: Metric;

    graph_formula_provider!(
        (grid),
        (consumer),
        (producer),
        (battery, ids: _battery_ids),
        (chp, ids: _chp_ids),
        (pv, ids: _pv_inverter_ids),
        (ev_charger, ids: _ev_charger_ids),
        (component, id: _component_id),
    );
}

macro_rules! impl_graph_formula_provider {
    ($((
        $fnname:ident,
        $graphfnname:ident
        $(, ids:$idsparam:ident)?
        $(, id:$idparam:ident)?
    )),+ $(,)?) => {$(

        fn $fnname(
            graph: &ComponentGraph<ElectricalComponent, ElectricalComponentConnection>,
            _metric: Self::MetricType,
            instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
            $($idsparam: Option<BTreeSet<u64>>,)?
            $($idparam: u64,)?
        ) -> Result<Self, Error> {
            let formula = graph.$graphfnname($($idsparam)?$($idparam)?).map_err(|e| {
                Error::component_graph_error(
                    format!("Could not get {} formula: {e}", stringify!($fnname))
                )
            })?;
            Ok(FormulaParams::new(formula, _metric, instructions_tx).into())
        }

    )+};
}

impl<M: Metric> GraphFormulaProvider for AggregationFormula<M> {
    type MetricType = M;

    impl_graph_formula_provider!(
        (grid, grid_formula),
        (consumer, consumer_formula),
        (producer, producer_formula),
        (battery, battery_formula, ids: battery_ids),
        (chp, chp_formula, ids: chp_ids),
        (pv, pv_formula, ids: pv_inverter_ids),
        (ev_charger, ev_charger_formula, ids: ev_charger_ids),
        (component, component_formula, id: component_id),
    );
}

impl<M: Metric> GraphFormulaProvider for CoalesceFormula<M> {
    type MetricType = M;

    impl_graph_formula_provider!(
        (grid, grid_coalesce_formula),
        (battery, battery_ac_coalesce_formula, ids: battery_ids),
        (pv, pv_ac_coalesce_formula, ids: pv_inverter_ids),
        (component, component_ac_coalesce_formula, id: component_id),
    );
}
