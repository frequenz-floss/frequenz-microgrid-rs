// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

use crate::logical_meter::formula::Formula;
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
            client
                .list_electrical_component_connections(vec![], vec![])
                .await?,
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
    pub fn grid<M: super::metric::Metric>(
        &mut self,
        metric: M,
    ) -> Result<Formula<M::QuantityType>, Error> {
        Ok(Formula::Subscriber(Box::new(M::FormulaType::grid(
            &self.graph,
            metric,
            self.instructions_tx.clone(),
        )?)))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given battery IDs.
    ///
    /// When `component_ids` is `None`, all batteries in the microgrid are used.
    pub fn battery<M: super::metric::Metric>(
        &mut self,
        component_ids: Option<BTreeSet<u64>>,
        metric: M,
    ) -> Result<Formula<M::QuantityType>, Error> {
        Ok(Formula::Subscriber(Box::new(M::FormulaType::battery(
            &self.graph,
            metric,
            self.instructions_tx.clone(),
            component_ids,
        )?)))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given CHP IDs.
    ///
    /// When `component_ids` is `None`, all CHPs in the microgrid are used.
    pub fn chp<M: super::metric::Metric>(
        &mut self,
        component_ids: Option<BTreeSet<u64>>,
        metric: M,
    ) -> Result<Formula<M::QuantityType>, Error> {
        Ok(Formula::Subscriber(Box::new(M::FormulaType::chp(
            &self.graph,
            metric,
            self.instructions_tx.clone(),
            component_ids,
        )?)))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given PV IDs.
    ///
    /// When `component_ids` is `None`, all PVs in the microgrid are used.
    pub fn pv<M: super::metric::Metric>(
        &mut self,
        component_ids: Option<BTreeSet<u64>>,
        metric: M,
    ) -> Result<Formula<M::QuantityType>, Error> {
        Ok(Formula::Subscriber(Box::new(M::FormulaType::pv(
            &self.graph,
            metric,
            self.instructions_tx.clone(),
            component_ids,
        )?)))
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
    ) -> Result<Formula<M::QuantityType>, Error> {
        Ok(Formula::Subscriber(Box::new(M::FormulaType::ev_charger(
            &self.graph,
            metric,
            self.instructions_tx.clone(),
            component_ids,
        )?)))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// logical `consumer` in the microgrid.
    pub fn consumer<M: super::metric::Metric>(
        &mut self,
        metric: M,
    ) -> Result<Formula<M::QuantityType>, Error> {
        Ok(Formula::Subscriber(Box::new(M::FormulaType::consumer(
            &self.graph,
            metric,
            self.instructions_tx.clone(),
        )?)))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// logical `producer` in the microgrid.
    pub fn producer<M: super::metric::Metric>(
        &mut self,
        metric: M,
    ) -> Result<Formula<M::QuantityType>, Error> {
        Ok(Formula::Subscriber(Box::new(M::FormulaType::producer(
            &self.graph,
            metric,
            self.instructions_tx.clone(),
        )?)))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given component ID.
    pub fn component<M: super::metric::Metric>(
        &mut self,
        component_id: u64,
        metric: M,
    ) -> Result<Formula<M::QuantityType>, Error> {
        Ok(Formula::Subscriber(Box::new(M::FormulaType::component(
            &self.graph,
            metric,
            self.instructions_tx.clone(),
            component_id,
        )?)))
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use tokio_stream::{StreamExt, wrappers::BroadcastStream};

    use crate::{
        LogicalMeterConfig, LogicalMeterHandle, MicrogridClientHandle, Sample,
        client::test_utils::{
            MockComponent,
            MockMicrogridApiClient, //
        },
        logical_meter::formula::Formula,
        quantity::Quantity,
    };

    async fn new_logical_meter_handle() -> LogicalMeterHandle {
        let api_client = MockMicrogridApiClient::new(
            // Grid connection point
            MockComponent::grid(1).with_children(vec![
                // Main meter
                MockComponent::meter(2)
                    .with_power(vec![4.0, 5.0, 6.0, 7.0, 7.0, 7.0])
                    .with_current(vec![1.0, 1.5, 2.0, 2.5, 2.0, 1.5])
                    .with_children(vec![
                        // PV meter
                        MockComponent::meter(3)
                            .with_reactive_power(vec![-2.0, -5.0, -4.0, 1.0, 3.0, 4.0])
                            .with_children(vec![
                                // PV inverter
                                MockComponent::pv_inverter(4),
                            ]),
                        // Battery meter
                        MockComponent::meter(5).with_children(vec![
                            // Battery inverter
                            MockComponent::battery_inverter(6)
                                .with_voltage(vec![400.0, 400.0, 398.0, 396.0, 396.0, 396.0])
                                .with_children(vec![
                                    // Battery
                                    MockComponent::battery(7),
                                ]),
                        ]),
                        // Consumer meter
                        MockComponent::meter(8)
                            .with_current(vec![14.5, 15.0, 16.0, 15.5, 14.0, 13.5]),
                    ]),
            ]),
        );

        LogicalMeterHandle::try_new(
            MicrogridClientHandle::new_from_client(api_client),
            LogicalMeterConfig {
                resampling_interval: TimeDelta::try_seconds(1).unwrap(),
            },
        )
        .await
        .unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn test_grid_power_formula() {
        let formula = new_logical_meter_handle()
            .await
            .grid(crate::metric::AcPowerActive)
            .unwrap();

        let samples = fetch_samples(formula, 10).await;

        check_samples(
            samples,
            |q| q.as_watts(),
            vec![
                Some(5.8),
                Some(6.0),
                Some(6.0),
                Some(7.0),
                Some(5.8),
                Some(6.0),
                Some(6.0),
                Some(7.0),
                Some(5.8),
                Some(6.0),
            ],
        )
    }

    #[tokio::test(start_paused = true)]
    async fn test_pv_reactive_power_formula() {
        let formula = new_logical_meter_handle()
            .await
            .pv(None, crate::metric::AcPowerReactive)
            .unwrap();

        let samples = fetch_samples(formula, 10).await;

        check_samples(
            samples,
            |q| q.as_volt_amperes_reactive(),
            vec![
                Some(-1.4),
                Some(-0.5),
                Some(-0.5),
                Some(4.0),
                Some(-1.4),
                Some(-0.5),
                Some(-0.5),
                Some(4.0),
                Some(-1.4),
                Some(-0.5),
            ],
        )
    }

    #[tokio::test(start_paused = true)]
    async fn test_battery_voltage_formula() {
        let formula = new_logical_meter_handle()
            .await
            .battery(None, crate::metric::AcVoltage)
            .unwrap();

        let samples = fetch_samples(formula, 10).await;
        check_samples(
            samples,
            |q| q.as_volts(),
            vec![
                Some(398.0),
                Some(397.67),
                Some(397.67),
                Some(396.0),
                Some(398.0),
                Some(397.67),
                Some(397.67),
                Some(396.0),
                Some(398.0),
                Some(397.67),
            ],
        )
    }

    #[tokio::test(start_paused = true)]
    async fn test_consumer_current_formula() {
        let formula = new_logical_meter_handle()
            .await
            .consumer(crate::metric::AcCurrent)
            .unwrap();

        let samples = fetch_samples(formula, 10).await;
        check_samples(
            samples,
            |q| q.as_amperes(),
            vec![
                Some(15.0),
                Some(14.75),
                Some(14.75),
                Some(13.5),
                Some(15.0),
                Some(14.75),
                Some(14.75),
                Some(13.5),
                Some(15.0),
                Some(14.75),
            ],
        )
    }

    async fn fetch_samples<Q: Quantity>(formula: Formula<Q>, num_values: usize) -> Vec<Sample<Q>> {
        let rx = formula.subscribe().await.unwrap();

        BroadcastStream::new(rx)
            .take(num_values)
            .map(|x| x.unwrap())
            .collect()
            .await
    }

    #[track_caller]
    fn check_samples<Q: Quantity>(
        samples: Vec<Sample<Q>>,
        extractor: impl Fn(Q) -> f32,
        expected_values: Vec<Option<f32>>,
    ) {
        let values = samples
            .iter()
            .map(|res| res.value().map(|v| extractor(v)))
            .collect::<Vec<_>>();

        let one_second = TimeDelta::try_seconds(1).unwrap();

        samples.as_slice().windows(2).for_each(|w| {
            assert_eq!(w[1].timestamp() - w[0].timestamp(), one_second);
        });

        for (v, ev) in values.iter().zip(expected_values.iter()) {
            match (v, ev) {
                (Some(v), Some(ev)) => assert!(
                    (v - ev).abs() < 0.01,
                    "expected value {ev:?}, got value {v:?}"
                ),
                (None, None) => {}
                _ => panic!("expected value {ev:?}, got value {v:?}"),
            }
        }
    }
}
