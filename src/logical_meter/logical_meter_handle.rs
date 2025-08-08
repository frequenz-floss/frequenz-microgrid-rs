// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

use crate::logical_meter::formula::graph_formula_provider::GraphFormulaProvider;
use crate::{
    client::MicrogridClientHandle,
    error::Error,
    proto::common::v1alpha8::microgrid::electrical_components::{
        ElectricalComponent, ElectricalComponentConnection,
    },
};
use frequenz_microgrid_component_graph::{self, ComponentGraph};
use std::collections::BTreeSet;
use tokio::sync::mpsc;

use super::{LogicalMeterConfig, logical_meter_actor::LogicalMeterActor};

/// This provides an interface  stream high-level metrics from a microgrid.
#[derive(Clone)]
pub struct LogicalMeterHandle {
    instructions_tx: mpsc::Sender<super::logical_meter_actor::Instruction>,
    graph: ComponentGraph<ElectricalComponent, ElectricalComponentConnection>,
}

impl LogicalMeterHandle {
    /// Creates a new LogicalMeter instance.
    pub async fn try_new(
        client: MicrogridClientHandle,
        config: LogicalMeterConfig,
    ) -> Result<Self, Error> {
        let (sender, receiver) = mpsc::channel(8);
        let graph = ComponentGraph::try_new(
            client.list_electrical_components(vec![], vec![]).await?,
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
    pub fn grid<M: super::metric::Metric>(&mut self, metric: M) -> Result<M::FormulaType, Error> {
        M::FormulaType::grid(&self.graph, metric, self.instructions_tx.clone())
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given battery IDs.
    ///
    /// When `component_ids` is `None`, all batteries in the microgrid are used.
    pub fn battery<M: super::metric::Metric>(
        &mut self,
        component_ids: Option<BTreeSet<u64>>,
        metric: M,
    ) -> Result<M::FormulaType, Error> {
        M::FormulaType::battery(
            &self.graph,
            metric,
            self.instructions_tx.clone(),
            component_ids,
        )
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given CHP IDs.
    ///
    /// When `component_ids` is `None`, all CHPs in the microgrid are used.
    pub fn chp<M: super::metric::Metric>(
        &mut self,
        component_ids: Option<BTreeSet<u64>>,
        metric: M,
    ) -> Result<M::FormulaType, Error> {
        M::FormulaType::chp(
            &self.graph,
            metric,
            self.instructions_tx.clone(),
            component_ids,
        )
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given PV IDs.
    ///
    /// When `component_ids` is `None`, all PVs in the microgrid are used.
    pub fn pv<M: super::metric::Metric>(
        &mut self,
        component_ids: Option<BTreeSet<u64>>,
        metric: M,
    ) -> Result<M::FormulaType, Error> {
        M::FormulaType::pv(
            &self.graph,
            metric,
            self.instructions_tx.clone(),
            component_ids,
        )
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given EV charger IDs.
    ///
    /// When `component_ids` is `None`, all EV chargers in the microgrid are
    /// used.
    pub fn ev_charger<M: super::metric::Metric>(
        &mut self,
        component_ids: Option<BTreeSet<u64>>,
        metric: M,
    ) -> Result<M::FormulaType, Error> {
        M::FormulaType::ev_charger(
            &self.graph,
            metric,
            self.instructions_tx.clone(),
            component_ids,
        )
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// logical `consumer` in the microgrid.
    pub fn consumer<M: super::metric::Metric>(
        &mut self,
        metric: M,
    ) -> Result<M::FormulaType, Error> {
        M::FormulaType::consumer(&self.graph, metric, self.instructions_tx.clone())
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// logical `producer` in the microgrid.
    pub fn producer<M: super::metric::Metric>(
        &mut self,
        metric: M,
    ) -> Result<M::FormulaType, Error> {
        M::FormulaType::producer(&self.graph, metric, self.instructions_tx.clone())
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given component ID.
    pub fn component<M: super::metric::Metric>(
        &mut self,
        component_id: u64,
        metric: M,
    ) -> Result<M::FormulaType, Error> {
        M::FormulaType::component(
            &self.graph,
            metric,
            self.instructions_tx.clone(),
            component_id,
        )
    }
}
