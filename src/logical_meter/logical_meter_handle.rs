// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

use crate::client::proto::common::metrics::Metric as MetricPb;
use crate::logical_meter::formula::{Formula, Key};
use crate::metric::{FormulaKind, Metric};
use crate::{
    client::MicrogridClientHandle,
    client::proto::common::microgrid::electrical_components::{
        ElectricalComponent, ElectricalComponentConnection,
    },
    error::Error,
};
use frequenz_microgrid_component_graph::{self, ComponentGraph, ComponentGraphConfig};
use frequenz_microgrid_formula_engine::parse;
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

/// Which component-graph formula a handle method asks for.
enum GraphRequest {
    Grid,
    Consumer,
    Producer,
    Battery(Option<BTreeSet<u64>>),
    Chp(Option<BTreeSet<u64>>),
    Pv(Option<BTreeSet<u64>>),
    EvCharger(Option<BTreeSet<u64>>),
    Component(u64),
}

impl GraphRequest {
    fn name(&self) -> &'static str {
        match self {
            GraphRequest::Grid => "grid",
            GraphRequest::Consumer => "consumer",
            GraphRequest::Producer => "producer",
            GraphRequest::Battery(_) => "battery",
            GraphRequest::Chp(_) => "chp",
            GraphRequest::Pv(_) => "pv",
            GraphRequest::EvCharger(_) => "ev_charger",
            GraphRequest::Component(_) => "component",
        }
    }
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
    pub fn grid<M: Metric>(&self) -> Result<Formula<M::QuantityType>, Error> {
        self.formula::<M>(GraphRequest::Grid)
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given battery IDs.
    ///
    /// When `component_ids` is `None`, all batteries in the microgrid are used.
    pub fn battery<M: Metric>(
        &self,
        component_ids: Option<BTreeSet<u64>>,
    ) -> Result<Formula<M::QuantityType>, Error> {
        self.formula::<M>(GraphRequest::Battery(component_ids))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given CHP IDs.
    ///
    /// When `component_ids` is `None`, all CHPs in the microgrid are used.
    pub fn chp<M: Metric>(
        &self,
        component_ids: Option<BTreeSet<u64>>,
    ) -> Result<Formula<M::QuantityType>, Error> {
        self.formula::<M>(GraphRequest::Chp(component_ids))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given PV IDs.
    ///
    /// When `component_ids` is `None`, all PVs in the microgrid are used.
    pub fn pv<M: Metric>(
        &self,
        component_ids: Option<BTreeSet<u64>>,
    ) -> Result<Formula<M::QuantityType>, Error> {
        self.formula::<M>(GraphRequest::Pv(component_ids))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given EV charger IDs.
    ///
    /// When `component_ids` is `None`, all EV chargers in the microgrid are
    /// used.
    pub fn ev_charger<M: Metric>(
        &self,
        component_ids: Option<BTreeSet<u64>>,
    ) -> Result<Formula<M::QuantityType>, Error> {
        self.formula::<M>(GraphRequest::EvCharger(component_ids))
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// logical `consumer` in the microgrid.
    pub fn consumer<M: Metric>(&self) -> Result<Formula<M::QuantityType>, Error> {
        self.formula::<M>(GraphRequest::Consumer)
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// logical `producer` in the microgrid.
    pub fn producer<M: Metric>(&self) -> Result<Formula<M::QuantityType>, Error> {
        self.formula::<M>(GraphRequest::Producer)
    }

    /// Returns a receiver that streams samples for the given `metric` for the
    /// given component ID.
    ///
    /// For a component whose operational mode provides no telemetry
    /// (`Inactive` or `ControlOnly`), the formula has no reading.
    pub fn component<M: Metric>(
        &self,
        component_id: u64,
    ) -> Result<Formula<M::QuantityType>, Error> {
        self.formula::<M>(GraphRequest::Component(component_id))
    }

    /// Asks the graph for the formula selected by `M::KIND` and `request`,
    /// parses it, and tags every component leaf with `M::METRIC`.
    fn formula<M: Metric>(&self, request: GraphRequest) -> Result<Formula<M::QuantityType>, Error> {
        let name = request.name();
        let graph = &self.graph;
        let generated = match (M::KIND, request) {
            (FormulaKind::Aggregation, GraphRequest::Grid) => graph.grid_formula(),
            (FormulaKind::Aggregation, GraphRequest::Consumer) => graph.consumer_formula(),
            (FormulaKind::Aggregation, GraphRequest::Producer) => graph.producer_formula(),
            (FormulaKind::Aggregation, GraphRequest::Battery(ids)) => graph.battery_formula(ids),
            (FormulaKind::Aggregation, GraphRequest::Chp(ids)) => graph.chp_formula(ids),
            (FormulaKind::Aggregation, GraphRequest::Pv(ids)) => graph.pv_formula(ids),
            (FormulaKind::Aggregation, GraphRequest::EvCharger(ids)) => {
                graph.ev_charger_formula(ids)
            }
            (FormulaKind::Aggregation, GraphRequest::Component(id)) => graph.component_formula(id),
            (FormulaKind::Coalesce, GraphRequest::Grid) => graph.grid_coalesce_formula(),
            (FormulaKind::Coalesce, GraphRequest::Battery(ids)) => {
                graph.battery_ac_coalesce_formula(ids)
            }
            (FormulaKind::Coalesce, GraphRequest::Pv(ids)) => graph.pv_ac_coalesce_formula(ids),
            (FormulaKind::Coalesce, GraphRequest::Component(id)) => {
                graph.component_ac_coalesce_formula(id)
            }
            (FormulaKind::Coalesce, _) => {
                return Err(Error::component_graph_error(format!(
                    "The component graph does not support {name} formula generation for {}.",
                    M::str_name()
                )));
            }
        };
        let generated = generated.map_err(|e| {
            Error::component_graph_error(format!("Could not get {name} formula: {e}"))
        })?;
        Ok(Formula::new(
            tag_components(&generated, M::METRIC)?,
            self.instructions_tx.clone(),
        ))
    }

    /// Returns a reference to the component graph.
    pub fn graph(&self) -> &ComponentGraph<ElectricalComponent, ElectricalComponentConnection> {
        &self.graph
    }
}

/// Parses a graph formula and tags every component leaf with `metric`.
fn tag_components(
    generated: &frequenz_microgrid_component_graph::Formula,
    metric: MetricPb,
) -> Result<crate::FormulaExpr, Error> {
    Ok(parse::<f32>(&generated.to_string())
        .map_err(|e| Error::formula_engine_error(format!("Failed to parse formula: {e}")))?
        .map_components(|component_id| Key {
            metric,
            component_id,
        }))
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
        assert_eq!(formula.to_string(), "#2:AC_POWER_ACTIVE");

        let formula = lm.battery::<crate::metric::AcPowerReactive>(None).unwrap();
        assert_eq!(
            formula.to_string(),
            concat!(
                "COALESCE(#8:AC_POWER_REACTIVE + #6:AC_POWER_REACTIVE, #5:AC_POWER_REACTIVE, ",
                "COALESCE(#8:AC_POWER_REACTIVE, 0) + COALESCE(#6:AC_POWER_REACTIVE, 0))"
            )
        );

        let formula = lm
            .battery::<crate::metric::AcPowerActive>(Some([9].into()))
            .unwrap();
        assert_eq!(
            formula.to_string(),
            "COALESCE(#8:AC_POWER_ACTIVE, #5:AC_POWER_ACTIVE - #6:AC_POWER_ACTIVE, 0)"
        );

        let formula = lm
            .battery::<crate::metric::AcVoltage>(Some([7].into()))
            .unwrap();
        assert_eq!(
            formula.to_string(),
            "COALESCE(#5:AC_VOLTAGE, #6:AC_VOLTAGE)"
        );

        let formula = lm.battery::<crate::metric::AcFrequency>(None).unwrap();
        assert_eq!(
            formula.to_string(),
            "COALESCE(#5:AC_FREQUENCY, #6:AC_FREQUENCY, #8:AC_FREQUENCY)"
        );

        let formula = lm.pv::<crate::metric::AcPowerReactive>(None).unwrap();
        assert_eq!(
            formula.to_string(),
            "COALESCE(#4:AC_POWER_REACTIVE, #3:AC_POWER_REACTIVE, 0)"
        );

        let formula = lm.chp::<crate::metric::AcPowerActive>(None).unwrap();
        assert_eq!(
            formula.to_string(),
            "COALESCE(#12:AC_POWER_ACTIVE, #11:AC_POWER_ACTIVE, 0)"
        );

        let formula = lm.ev_charger::<crate::metric::AcCurrent>(None).unwrap();
        assert_eq!(
            formula.to_string(),
            concat!(
                "COALESCE(#15:AC_CURRENT + #14:AC_CURRENT, #13:AC_CURRENT, ",
                "COALESCE(#15:AC_CURRENT, 0) + COALESCE(#14:AC_CURRENT, 0))"
            )
        );

        let formula = lm.consumer::<crate::metric::AcCurrent>().unwrap();
        assert_eq!(
            formula.to_string(),
            rendered(
                &lm.graph().consumer_formula().unwrap(),
                super::MetricPb::AcCurrent
            )
        );
        // The outermost node is the phantom-load sum, so the `MAX` that
        // clamps the consumer total is nested inside it.
        assert!(formula.to_string().contains("MAX("), "{formula}");
        assert!(formula.to_string().contains("#10:AC_CURRENT"), "{formula}");

        let formula = lm.producer::<crate::metric::AcPowerActive>().unwrap();
        assert_eq!(
            formula.to_string(),
            rendered(
                &lm.graph().producer_formula().unwrap(),
                super::MetricPb::AcPowerActive
            )
        );
        assert!(formula.to_string().starts_with("MIN("), "{formula}");
        assert!(
            formula.to_string().contains("#12:AC_POWER_ACTIVE"),
            "{formula}"
        );

        let formula = lm.component::<crate::metric::AcCurrent>(10).unwrap();
        assert_eq!(formula.to_string(), "#10:AC_CURRENT");

        // The coalesce kind is only defined for grid, battery, pv and
        // component; the other categories report it.
        let Err(err) = lm.consumer::<crate::metric::AcVoltage>() else {
            panic!("expected no consumer coalesce formula");
        };
        assert!(
            err.to_string()
                .contains("does not support consumer formula generation"),
            "{err}"
        );
        let Err(err) = lm.ev_charger::<crate::metric::AcFrequency>(None) else {
            panic!("expected no ev_charger coalesce formula");
        };
        assert!(
            err.to_string()
                .contains("does not support ev_charger formula generation"),
            "{err}"
        );
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
                "COALESCE(#3:AC_POWER_ACTIVE, #5:AC_POWER_ACTIVE, 0)",
                "{mode:?}"
            );

            assert_eq!(
                lm.component::<crate::metric::AcPowerActive>(4)
                    .unwrap()
                    .to_string(),
                "None",
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

    #[tokio::test(start_paused = true)]
    async fn test_composed_formula_tracks_its_operands() {
        let lm = new_logical_meter_handle(None).await;
        let grid = lm.grid::<crate::metric::AcPowerActive>().unwrap();
        let doubled = grid.clone() + grid.clone();
        let scaled = grid.clone() * 2.0;
        let offset = grid.clone() + crate::quantity::Power::from_watts(1.0);
        let capped = grid.clone().min(crate::quantity::Power::from_watts(6.0));
        assert_eq!(
            doubled.to_string(),
            "#2:AC_POWER_ACTIVE + #2:AC_POWER_ACTIVE"
        );
        assert_eq!(scaled.to_string(), "#2:AC_POWER_ACTIVE * 2");
        assert_eq!(capped.to_string(), "MIN(#2:AC_POWER_ACTIVE, 6)");

        let (base, doubled, scaled, offset, capped) = tokio::join!(
            fetch_samples(grid, 5),
            fetch_samples(doubled, 5),
            fetch_samples(scaled, 5),
            fetch_samples(offset, 5),
            fetch_samples(capped, 5),
        );
        for i in 0..5 {
            let b = base[i].value().unwrap().as_watts();
            assert_eq!(doubled[i].timestamp(), base[i].timestamp());
            assert!((doubled[i].value().unwrap().as_watts() - 2.0 * b).abs() < 1e-3);
            assert!((scaled[i].value().unwrap().as_watts() - 2.0 * b).abs() < 1e-3);
            assert!((offset[i].value().unwrap().as_watts() - (b + 1.0)).abs() < 1e-3);
            assert!((capped[i].value().unwrap().as_watts() - b.min(6.0)).abs() < 1e-3);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_mixed_metric_formula() {
        let lm = new_logical_meter_handle(None).await;
        let voltage = lm.battery::<crate::metric::AcVoltage>(None).unwrap();
        let averaged = voltage.clone().avg(vec![voltage.clone()]) / 2.0
            + crate::quantity::Voltage::from_volts(0.0);
        // Nested operators are parenthesised by the engine's Display;
        // function arguments are not.
        let v = voltage.to_string();
        assert!(v.starts_with("COALESCE(#"), "{v}");
        assert_eq!(averaged.to_string(), format!("(AVG({v}, {v}) / 2) + 0"));
        let (base, averaged) = tokio::join!(fetch_samples(voltage, 4), fetch_samples(averaged, 4));
        for i in 0..4 {
            let b = base[i].value().unwrap().as_volts();
            assert!((averaged[i].value().unwrap().as_volts() - b / 2.0).abs() < 1e-3);
        }
    }

    /// Renders a graph formula the way the handle does, for wiring checks
    /// on formulas too long to spell out.
    fn rendered(
        formula: &frequenz_microgrid_component_graph::Formula,
        metric: super::MetricPb,
    ) -> String {
        super::tag_components(formula, metric).unwrap().to_string()
    }

    async fn fetch_samples<Q: Quantity + 'static>(
        formula: Formula<Q>,
        num_values: usize,
    ) -> Vec<Sample<Q>> {
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
