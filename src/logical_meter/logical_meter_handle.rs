// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

use crate::logical_meter::formula::Formula;
use crate::logical_meter::formula::graph_formula_provider::GraphFormulaProvider;
use crate::{
    client::MicrogridClientHandle,
    client::proto::common::microgrid::electrical_components::{
        ElectricalComponent, ElectricalComponentConnection,
    },
    error::Error,
    metric,
};
use frequenz_microgrid_component_graph::{self, ComponentGraph, ComponentGraphConfig};
use std::collections::BTreeSet;
use std::time::Duration;
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
    ///
    /// Listing the components and connections from the API and building the
    /// component graph is retried indefinitely with a 3 second backoff, so
    /// this call blocks until the server is reachable and returns data that
    /// forms a valid graph.  Returns an error only if `config` is invalid.
    pub async fn try_new(
        client: MicrogridClientHandle,
        config: LogicalMeterConfig,
    ) -> Result<Self, Error> {
        Self::try_new_with_clock(client, config, crate::wall_clock_timer::SystemClock).await
    }

    pub(crate) async fn try_new_with_clock<C: crate::wall_clock_timer::Clock + 'static>(
        client: MicrogridClientHandle,
        config: LogicalMeterConfig,
        clock: C,
    ) -> Result<Self, Error> {
        let (sender, receiver) = mpsc::channel(8);
        const RETRY_DELAY: Duration = Duration::from_secs(3);
        let graph = loop {
            match build_component_graph(&client, &config.component_graph_config).await {
                Ok(g) => break g,
                Err(reason) => {
                    tracing::warn!(
                        "Microgrid logical-meter setup failed, retrying in {:?}: {reason}",
                        RETRY_DELAY
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        };

        let logical_meter = LogicalMeterActor::try_new(receiver, client, config, clock)?;

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
    pub fn grid<M: metric::Metric>(&self) -> Result<Formula<M::QuantityType>, Error> {
        Ok(Formula::Subscriber(Box::new(M::FormulaType::grid(
            &self.graph,
            self.instructions_tx.clone(),
        )?)))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given battery IDs.
    ///
    /// When `component_ids` is `None`, all batteries in the microgrid are used.
    pub fn battery<M: metric::Metric>(
        &self,
        component_ids: Option<BTreeSet<u64>>,
    ) -> Result<Formula<M::QuantityType>, Error> {
        Ok(Formula::Subscriber(Box::new(M::FormulaType::battery(
            &self.graph,
            self.instructions_tx.clone(),
            component_ids,
        )?)))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given CHP IDs.
    ///
    /// When `component_ids` is `None`, all CHPs in the microgrid are used.
    pub fn chp<M: metric::Metric>(
        &self,
        component_ids: Option<BTreeSet<u64>>,
    ) -> Result<Formula<M::QuantityType>, Error> {
        Ok(Formula::Subscriber(Box::new(M::FormulaType::chp(
            &self.graph,
            self.instructions_tx.clone(),
            component_ids,
        )?)))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given PV IDs.
    ///
    /// When `component_ids` is `None`, all PVs in the microgrid are used.
    pub fn pv<M: metric::Metric>(
        &self,
        component_ids: Option<BTreeSet<u64>>,
    ) -> Result<Formula<M::QuantityType>, Error> {
        Ok(Formula::Subscriber(Box::new(M::FormulaType::pv(
            &self.graph,
            self.instructions_tx.clone(),
            component_ids,
        )?)))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given EV charger IDs.
    ///
    /// When `component_ids` is `None`, all EV chargers in the microgrid are
    /// used.
    pub fn ev_charger<M: metric::Metric>(
        &self,
        component_ids: Option<BTreeSet<u64>>,
    ) -> Result<Formula<M::QuantityType>, Error> {
        Ok(Formula::Subscriber(Box::new(M::FormulaType::ev_charger(
            &self.graph,
            self.instructions_tx.clone(),
            component_ids,
        )?)))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given steam boiler IDs.
    ///
    /// When `component_ids` is `None`, all steam boilers in the microgrid are
    /// used.
    pub fn steam_boiler<M: metric::Metric>(
        &self,
        component_ids: Option<BTreeSet<u64>>,
    ) -> Result<Formula<M::QuantityType>, Error> {
        Ok(Formula::Subscriber(Box::new(M::FormulaType::steam_boiler(
            &self.graph,
            self.instructions_tx.clone(),
            component_ids,
        )?)))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// logical `consumer` in the microgrid.
    pub fn consumer<M: metric::Metric>(&self) -> Result<Formula<M::QuantityType>, Error> {
        Ok(Formula::Subscriber(Box::new(M::FormulaType::consumer(
            &self.graph,
            self.instructions_tx.clone(),
        )?)))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// logical `producer` in the microgrid.
    pub fn producer<M: metric::Metric>(&self) -> Result<Formula<M::QuantityType>, Error> {
        Ok(Formula::Subscriber(Box::new(M::FormulaType::producer(
            &self.graph,
            self.instructions_tx.clone(),
        )?)))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given component ID.
    ///
    /// For a component whose operational mode provides no telemetry
    /// (`Inactive` or `ControlOnly`), the formula has no reading.
    pub fn component<M: metric::Metric>(
        &self,
        component_id: u64,
    ) -> Result<Formula<M::QuantityType>, Error> {
        Ok(Formula::Subscriber(Box::new(M::FormulaType::component(
            &self.graph,
            self.instructions_tx.clone(),
            component_id,
        )?)))
    }

    /// Returns a reference to the component graph.
    pub fn graph(&self) -> &ComponentGraph<ElectricalComponent, ElectricalComponentConnection> {
        &self.graph
    }
}

/// Lists the components and connections from the API and builds the
/// component graph.  Errors from each step are stringified with a prefix so
/// the retry loop can log a concise reason.
async fn build_component_graph(
    client: &MicrogridClientHandle,
    config: &ComponentGraphConfig,
) -> Result<ComponentGraph<ElectricalComponent, ElectricalComponentConnection>, String> {
    let components = client
        .list_electrical_components(vec![], vec![])
        .await
        .map_err(|e| format!("fetching components failed: {e}"))?;
    let connections = client
        .list_electrical_component_connections(vec![], vec![])
        .await
        .map_err(|e| format!("fetching component connections failed: {e}"))?;
    ComponentGraph::try_new(components, connections, config.clone())
        .map_err(|e| format!("building component graph failed: {e}"))
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use frequenz_resampling::ResamplingFunction;
    use tokio_stream::{StreamExt, wrappers::BroadcastStream};

    use frequenz_microgrid_component_graph::ComponentGraphConfig;

    use crate::{
        LogicalMeterConfig, LogicalMeterHandle, MicrogridClientHandle, Sample,
        client::proto::common::microgrid::electrical_components::ElectricalComponentOperationalMode,
        client::test_utils::{
            MockComponent,
            MockMicrogridApiClient, //
        },
        logical_meter::formula::Formula,
        quantity::Quantity,
    };

    async fn new_logical_meter_handle(config: Option<LogicalMeterConfig>) -> LogicalMeterHandle {
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
                            // Battery inverter
                            MockComponent::battery_inverter(8)
                                .with_voltage(vec![400.0, 400.0, 398.0, 396.0, 396.0, 396.0])
                                .with_children(vec![
                                    // Battery
                                    MockComponent::battery(9),
                                ]),
                        ]),
                        // Consumer meter
                        MockComponent::meter(10)
                            .with_current(vec![14.5, 15.0, 16.0, 15.5, 14.0, 13.5]),
                        // Chp meter
                        MockComponent::meter(11).with_children(vec![
                            // Chp
                            MockComponent::chp(12),
                        ]),
                        // Ev charger meter
                        MockComponent::meter(13).with_children(vec![
                            // Ev chargers
                            MockComponent::ev_charger(14),
                            MockComponent::ev_charger(15),
                        ]),
                        // Steam boiler meter
                        MockComponent::meter(16).with_children(vec![
                            // Steam boiler
                            MockComponent::steam_boiler(17),
                        ]),
                    ]),
            ]),
        );

        let clock = api_client.clock();
        LogicalMeterHandle::try_new_with_clock(
            MicrogridClientHandle::new_from_client(api_client),
            config.unwrap_or_else(|| LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap())),
            clock,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_formula_display() {
        let lm = new_logical_meter_handle(Some(
            LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap())
                .with_component_graph_config(
                    ComponentGraphConfig::builder()
                        .prefer_meters_in_component_formulas(false)
                        .include_phantom_loads_in_consumer_formula(true)
                        .build(),
                ),
        ))
        .await;

        let formula = lm.grid::<crate::metric::AcPowerActive>().unwrap();
        assert_eq!(formula.to_string(), "METRIC_AC_POWER_ACTIVE::(#2)");

        let formula = lm.battery::<crate::metric::AcPowerReactive>(None).unwrap();
        assert_eq!(
            formula.to_string(),
            "METRIC_AC_POWER_REACTIVE::(COALESCE(#8 + #6, #5, COALESCE(#8, 0.0) + COALESCE(#6, 0.0)))"
        );

        let formula = lm
            .battery::<crate::metric::AcPowerActive>(Some([9].into()))
            .unwrap();
        assert_eq!(
            formula.to_string(),
            "METRIC_AC_POWER_ACTIVE::(COALESCE(#8, #5 - #6, 0.0))"
        );

        let formula = lm
            .battery::<crate::metric::AcVoltage>(Some([7].into()))
            .unwrap();
        assert_eq!(formula.to_string(), "METRIC_AC_VOLTAGE::(COALESCE(#5, #6))");

        let formula = lm.battery::<crate::metric::AcFrequency>(None).unwrap();
        assert_eq!(
            formula.to_string(),
            "METRIC_AC_FREQUENCY::(COALESCE(#5, #6, #8))"
        );

        let formula = lm.pv::<crate::metric::AcPowerReactive>(None).unwrap();
        assert_eq!(
            formula.to_string(),
            "METRIC_AC_POWER_REACTIVE::(COALESCE(#4, #3, 0.0))"
        );

        let formula = lm.chp::<crate::metric::AcPowerActive>(None).unwrap();
        assert_eq!(
            formula.to_string(),
            "METRIC_AC_POWER_ACTIVE::(COALESCE(#12, #11, 0.0))"
        );

        let formula = lm.ev_charger::<crate::metric::AcCurrent>(None).unwrap();
        assert_eq!(
            formula.to_string(),
            "METRIC_AC_CURRENT::(COALESCE(#15 + #14, #13, COALESCE(#15, 0.0) + COALESCE(#14, 0.0)))"
        );

        let formula = lm
            .steam_boiler::<crate::metric::AcPowerActive>(None)
            .unwrap();
        assert_eq!(
            formula.to_string(),
            "METRIC_AC_POWER_ACTIVE::(COALESCE(#17, #16, 0.0))"
        );

        let formula = lm
            .steam_boiler::<crate::metric::AcPowerActive>(Some([17].into()))
            .unwrap();
        assert_eq!(
            formula.to_string(),
            "METRIC_AC_POWER_ACTIVE::(COALESCE(#17, #16, 0.0))"
        );

        // 16 is the steam boiler's meter, not a steam boiler.
        let err = lm
            .steam_boiler::<crate::metric::AcPowerActive>(Some([16].into()))
            .err()
            .expect("a non-steam-boiler ID must be rejected");
        assert!(
            err.to_string().contains("is not a steam boiler"),
            "unexpected error: {err}"
        );

        // Only the aggregation path exists for steam boilers (as for CHP), so a
        // coalesce metric is unsupported.
        let err = lm
            .steam_boiler::<crate::metric::AcVoltage>(None)
            .err()
            .expect("coalesce metrics are unsupported for steam boilers");
        assert!(
            err.to_string()
                .contains("does not support steam_boiler formula generation"),
            "unexpected error: {err}"
        );

        let formula = lm.consumer::<crate::metric::AcCurrent>().unwrap();
        assert_eq!(
            formula.to_string(),
            concat!(
                "METRIC_AC_CURRENT::(MAX(",
                "#2 - COALESCE(#3, #4, 0.0) - COALESCE(#5, COALESCE(#8, 0.0) + COALESCE(#6, 0.0)) ",
                "- #10 - COALESCE(#11, #12, 0.0)",
                " - COALESCE(#13, COALESCE(#15, 0.0) + COALESCE(#14, 0.0))",
                " - COALESCE(#16, #17, 0.0),",
                " 0.0)",
                " + COALESCE(MAX(#3 - #4, 0.0), 0.0) + COALESCE(MAX(#5 - #6 - #8, 0.0), 0.0)",
                " + MAX(#10, 0.0) + COALESCE(MAX(#11 - #12, 0.0), 0.0)",
                " + COALESCE(MAX(#13 - #14 - #15, 0.0), 0.0)",
                " + COALESCE(MAX(#16 - #17, 0.0), 0.0)",
                ")"
            )
        );

        let formula = lm.producer::<crate::metric::AcPowerActive>().unwrap();
        assert_eq!(
            formula.to_string(),
            concat!(
                "METRIC_AC_POWER_ACTIVE::(",
                "MIN(COALESCE(#4, #3, 0.0), 0.0)",
                " + MIN(COALESCE(#12, #11, 0.0), 0.0)",
                ")"
            )
        );

        let formula = lm.component::<crate::metric::AcCurrent>(10).unwrap();
        assert_eq!(formula.to_string(), "METRIC_AC_CURRENT::(#10)");
    }

    #[tokio::test(start_paused = true)]
    async fn test_grid_power_formula() {
        let formula = new_logical_meter_handle(None)
            .await
            .grid::<crate::metric::AcPowerActive>()
            .unwrap();

        let samples = fetch_samples(formula, 10).await;

        check_samples(
            samples,
            |q| q.as_watts(),
            TimeDelta::try_seconds(1).unwrap(),
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
        let formula = new_logical_meter_handle(None)
            .await
            .pv::<crate::metric::AcPowerReactive>(None)
            .unwrap();

        let samples = fetch_samples(formula, 10).await;

        check_samples(
            samples,
            |q| q.as_volt_amperes_reactive(),
            TimeDelta::try_seconds(1).unwrap(),
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
        let formula = new_logical_meter_handle(None)
            .await
            .battery::<crate::metric::AcVoltage>(None)
            .unwrap();

        let samples = fetch_samples(formula, 10).await;
        check_samples(
            samples,
            |q| q.as_volts(),
            TimeDelta::try_seconds(1).unwrap(),
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
    async fn test_resampling_functions() {
        let lm_config = Some(
            LogicalMeterConfig::new(TimeDelta::try_milliseconds(200).unwrap())
                .with_default_resampling_function(ResamplingFunction::Count)
                .override_resampling_function::<crate::metric::AcVoltage>(ResamplingFunction::Last),
        );
        let lm = new_logical_meter_handle(lm_config).await;
        let bat_volt_formula = lm.battery::<crate::metric::AcVoltage>(None).unwrap();

        let samples = fetch_samples(bat_volt_formula, 10).await;
        check_samples(
            samples,
            |q| q.as_volts(),
            TimeDelta::try_milliseconds(200).unwrap(),
            vec![
                Some(400.0),
                Some(400.0),
                Some(398.0),
                Some(396.0),
                Some(396.0),
                Some(396.0),
                Some(396.0),
                Some(396.0),
                None,
                None,
            ],
        );

        let cons_pow_formula = lm.consumer::<crate::metric::AcPowerActive>().unwrap();

        let samples = fetch_samples(cons_pow_formula, 10).await;
        check_samples(
            samples,
            |q| q.as_watts(),
            TimeDelta::try_milliseconds(200).unwrap(),
            vec![
                Some(1.0),
                Some(2.0),
                Some(3.0),
                Some(3.0),
                Some(3.0),
                Some(3.0),
                Some(2.0),
                Some(1.0),
                Some(0.0),
                Some(0.0),
            ],
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_max_age_in_intervals() {
        let lm_config = Some(
            LogicalMeterConfig::new(TimeDelta::try_milliseconds(200).unwrap())
                .with_max_age_in_intervals(1)
                .with_default_resampling_function(ResamplingFunction::Count),
        );
        let lm = new_logical_meter_handle(lm_config).await;
        let formula = lm.consumer::<crate::metric::AcPowerActive>().unwrap();

        let samples = fetch_samples(formula, 8).await;
        check_samples(
            samples,
            |q| q.as_watts(),
            TimeDelta::try_milliseconds(200).unwrap(),
            vec![
                Some(1.0),
                Some(1.0),
                Some(1.0),
                Some(1.0),
                Some(1.0),
                Some(1.0),
                Some(0.0),
                Some(0.0),
            ],
        );

        let lm_config = Some(
            LogicalMeterConfig::new(TimeDelta::try_milliseconds(200).unwrap())
                .with_max_age_in_intervals(3)
                .with_default_resampling_function(ResamplingFunction::Count),
        );
        let lm = new_logical_meter_handle(lm_config).await;
        let formula = lm.consumer::<crate::metric::AcPowerActive>().unwrap();

        let samples = fetch_samples(formula, 10).await;
        check_samples(
            samples,
            |q| q.as_watts(),
            TimeDelta::try_milliseconds(200).unwrap(),
            vec![
                Some(1.0),
                Some(2.0),
                Some(3.0),
                Some(3.0),
                Some(3.0),
                Some(3.0),
                Some(2.0),
                Some(1.0),
                Some(0.0),
                Some(0.0),
            ],
        )
    }

    #[tokio::test(start_paused = true)]
    async fn test_consumer_current_formula() {
        let formula = new_logical_meter_handle(Some(
            LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap())
                .with_component_graph_config(
                    ComponentGraphConfig::builder()
                        .include_phantom_loads_in_consumer_formula(true)
                        .build(),
                ),
        ))
        .await
        .consumer::<crate::metric::AcCurrent>()
        .unwrap();

        let samples = fetch_samples(formula, 10).await;
        check_samples(
            samples,
            |q| q.as_amperes(),
            TimeDelta::try_seconds(1).unwrap(),
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

    /// `grid(1) -> meter(2) -> [meter(3) -> [pv_inverter(4), pv_inverter(5)]]`,
    /// with `mode`, when set, applied to inverter 4.
    fn pv_topology(mode: Option<ElectricalComponentOperationalMode>) -> MockComponent {
        let mut inverter_4 = MockComponent::pv_inverter(4);
        if let Some(mode) = mode {
            inverter_4 = inverter_4.with_operational_mode(mode);
        }
        MockComponent::grid(1).with_children(vec![MockComponent::meter(2).with_children(vec![
            MockComponent::meter(3).with_children(vec![inverter_4, MockComponent::pv_inverter(5)]),
        ])])
    }

    /// Builds a logical-meter handle over [`pv_topology`] with the given
    /// operational mode on inverter 4.
    async fn pv_handle(mode: Option<ElectricalComponentOperationalMode>) -> LogicalMeterHandle {
        crate::microgrid::test_utils::handles(pv_topology(mode))
            .await
            .1
    }

    #[tokio::test]
    async fn test_non_telemetry_modes_are_not_measurement_sources() {
        for mode in [
            ElectricalComponentOperationalMode::Inactive,
            ElectricalComponentOperationalMode::ControlOnly,
        ] {
            let lm = pv_handle(Some(mode)).await;

            // The inactive inverter has no reading, so no child sum is exact;
            // the graph keeps the meter as primary source and the remaining
            // inverter as best-effort fallback.
            assert_eq!(
                lm.pv::<crate::metric::AcPowerActive>(None)
                    .unwrap()
                    .to_string(),
                "METRIC_AC_POWER_ACTIVE::(COALESCE(#3, #5, 0.0))",
                "{mode:?}"
            );

            assert_eq!(
                lm.component::<crate::metric::AcPowerActive>(4)
                    .unwrap()
                    .to_string(),
                "METRIC_AC_POWER_ACTIVE::(None)",
                "{mode:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_telemetry_providing_modes_keep_the_formula() {
        let baseline = pv_handle(None)
            .await
            .pv::<crate::metric::AcPowerActive>(None)
            .unwrap()
            .to_string();
        assert!(baseline.contains("#4"), "{baseline}");

        for mode in [
            // Explicit Unspecified equals the unset field (prost default 0).
            ElectricalComponentOperationalMode::Unspecified,
            ElectricalComponentOperationalMode::TelemetryOnly,
            ElectricalComponentOperationalMode::ControlAndTelemetry,
        ] {
            assert_eq!(
                pv_handle(Some(mode))
                    .await
                    .pv::<crate::metric::AcPowerActive>(None)
                    .unwrap()
                    .to_string(),
                baseline,
                "{mode:?}"
            );
        }
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
        expected_interval: TimeDelta,
        expected_values: Vec<Option<f32>>,
    ) {
        let values = samples
            .iter()
            .map(|res| res.value().map(&extractor))
            .collect::<Vec<_>>();

        samples.as_slice().windows(2).for_each(|w| {
            assert_eq!(
                w[1].timestamp() - w[0].timestamp(),
                expected_interval,
                "Samples are not spaced at the expected interval"
            );
        });

        for (id, (v, ev)) in values.iter().zip(expected_values.iter()).enumerate() {
            match (v, ev) {
                (Some(v), Some(ev)) => assert!(
                    (v - ev).abs() < 0.01,
                    "Item {id} - expected value {ev:?}, got value {v:?}"
                ),
                (None, None) => {}
                _ => panic!("Item {id} - expected value {ev:?}, got value {v:?}"),
            }
        }
    }
}
