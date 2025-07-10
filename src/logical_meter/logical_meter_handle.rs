// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

use crate::{
    client::MicrogridClientHandle,
    error::Error,
    proto::common::v1::microgrid::components::{Component, ComponentConnection},
};
use frequenz_microgrid_component_graph::{self, ComponentGraph};
use std::collections::BTreeSet;
use tokio::sync::mpsc;

use super::{Formula, LogicalMeterConfig, Metric, logical_meter_actor::LogicalMeterActor};

/// This provides an interface  stream high-level metrics from a microgrid.
#[derive(Clone)]
pub struct LogicalMeterHandle {
    instructions_tx: mpsc::Sender<super::logical_meter_actor::Instruction>,
    graph: ComponentGraph<Component, ComponentConnection>,
}

impl LogicalMeterHandle {
    /// Creates a new LogicalMeter instance.
    pub async fn try_new(
        client: MicrogridClientHandle,
        config: LogicalMeterConfig,
    ) -> Result<Self, Error> {
        let (sender, receiver) = mpsc::channel(8);
        let graph = ComponentGraph::try_new(
            client.list_components(vec![], vec![]).await?,
            client.list_connections(vec![], vec![]).await?,
            frequenz_microgrid_component_graph::ComponentGraphConfig {
                allow_component_validation_failures: true,
                allow_unconnected_components: true,
                allow_unspecified_inverters: false,
                disable_fallback_components: false,
            },
        )
        .map_err(|e| {
            Error::component_graph_error(format!("Unable to create a component graph: {e}"))
        })?;

        let logical_meter = LogicalMeterActor::try_new(receiver, client, config)?;

        tokio::task::spawn(async move {
            logical_meter.run().await;
        });

        Ok(Self {
            instructions_tx: sender,
            graph,
        })
    }

    /// Returns a receiver that streams samples for the given `metric` at the grid
    /// connection point.
    pub fn grid(&mut self, metric: Metric) -> Result<Formula, Error> {
        if !metric.power() && !metric.current() {
            return Err(Error::invalid_metric(format!(
                "The grid formula only supports power or current metrics, but got: {metric:?}"
            )));
        }
        let formula = self.graph.grid_formula().map_err(|e| {
            Error::component_graph_error(format!("Could not derive grid formula: {e}"))
        })?;

        Ok(Formula::new(formula, metric, self.instructions_tx.clone()))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given battery IDs.
    ///
    /// When `component_ids` is `None`, all batteries in the microgrid are used.
    pub fn battery(
        &mut self,
        component_ids: Option<BTreeSet<u64>>,
        metric: Metric,
    ) -> Result<Formula, Error> {
        if !metric.power() && !metric.current() {
            return Err(Error::invalid_metric(format!(
                "The battery formula only supports power or current metrics, but got: {metric:?}"
            )));
        }
        let formula = self.graph.battery_formula(component_ids).map_err(|e| {
            Error::component_graph_error(format!("Could not derive battery formula: {e}"))
        })?;
        Ok(Formula::new(formula, metric, self.instructions_tx.clone()))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given CHP IDs.
    ///
    /// When `component_ids` is `None`, all CHPs in the microgrid are used.
    pub fn chp(
        &mut self,
        component_ids: Option<BTreeSet<u64>>,
        metric: Metric,
    ) -> Result<Formula, Error> {
        if !metric.power() && !metric.current() {
            return Err(Error::invalid_metric(format!(
                "The CHP formula only supports power or current metrics, but got: {metric:?}"
            )));
        }
        let formula = self.graph.chp_formula(component_ids).map_err(|e| {
            Error::component_graph_error(format!("Could not derive CHP formula: {e}"))
        })?;
        Ok(Formula::new(formula, metric, self.instructions_tx.clone()))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given PV IDs.
    ///
    /// When `component_ids` is `None`, all PVs in the microgrid are used.
    pub fn pv(
        &mut self,
        component_ids: Option<BTreeSet<u64>>,
        metric: Metric,
    ) -> Result<Formula, Error> {
        if !metric.power() && !metric.current() {
            return Err(Error::invalid_metric(format!(
                "The PV formula only supports power or current metrics, but got: {metric:?}"
            )));
        }
        let formula = self.graph.pv_formula(component_ids).map_err(|e| {
            Error::component_graph_error(format!("Could not derive PV formula: {e}"))
        })?;
        Ok(Formula::new(formula, metric, self.instructions_tx.clone()))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given EV charger IDs.
    ///
    /// When `component_ids` is `None`, all EV chargers in the microgrid are
    /// used.
    pub fn ev_charger(
        &mut self,
        component_ids: Option<BTreeSet<u64>>,
        metric: Metric,
    ) -> Result<Formula, Error> {
        if !metric.power() && !metric.current() {
            return Err(Error::invalid_metric(format!(
                "The EV charger formula only supports power or current metrics, but got: {metric:?}"
            )));
        }
        let formula = self.graph.ev_charger_formula(component_ids).map_err(|e| {
            Error::component_graph_error(format!("Could not derive EV charger formula: {e}"))
        })?;
        Ok(Formula::new(formula, metric, self.instructions_tx.clone()))
    }

    /// Returns a receiver that streams samples for the given `metric` for all
    /// the consumers in the microgrid.
    pub fn consumer(&mut self, metric: Metric) -> Result<Formula, Error> {
        if !metric.power() && !metric.current() {
            return Err(Error::invalid_metric(format!(
                "The consumer formula only supports power or current metrics, but got: {metric:?}"
            )));
        }
        let formula = self.graph.consumer_formula().map_err(|e| {
            Error::component_graph_error(format!("Could not derive consumer formula: {e}"))
        })?;
        Ok(Formula::new(formula, metric, self.instructions_tx.clone()))
    }

    /// Returns a receiver that streams samples for the given `metric` for all
    /// producers in the microgrid.
    pub fn producer(&mut self, metric: Metric) -> Result<Formula, Error> {
        if !metric.power() && !metric.current() {
            return Err(Error::invalid_metric(format!(
                "The producer formula only supports power or current metrics, but got: {metric:?}"
            )));
        }
        let formula = self.graph.producer_formula().map_err(|e| {
            Error::component_graph_error(format!("Could not derive producer formula: {e}"))
        })?;
        Ok(Formula::new(formula, metric, self.instructions_tx.clone()))
    }

    pub fn coalesce(
        &mut self,
        component_ids: BTreeSet<u64>,
        metric: Metric,
    ) -> Result<Formula, Error> {
        let formula = self.graph.coalesce(component_ids).map_err(|e| {
            Error::component_graph_error(format!("Could not derive coalesce formula: {e}"))
        })?;
        Ok(Formula::new(formula, metric, self.instructions_tx.clone()))
    }
}
