// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! A mock implementation of the MicrogridApiClient for testing.

mod tokio_synced_clock;
pub use tokio_synced_clock::TokioSyncedClock;

use std::sync::Mutex;
use std::{sync::Arc, time::SystemTime};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Response;

use super::MicrogridApiClient;
use crate::wall_clock_timer::Clock as _;
use crate::{
    client::proto::{
        common::{
            metrics::{
                Bounds, Metric, MetricSample, MetricValueVariant, SimpleMetricValue,
                metric_value_variant,
            },
            microgrid::electrical_components::{
                ElectricalComponent, ElectricalComponentCategory,
                ElectricalComponentCategorySpecificInfo, ElectricalComponentConnection,
                ElectricalComponentStateCode, ElectricalComponentStateSnapshot,
                ElectricalComponentTelemetry, Inverter, InverterType, MetricConfigBounds,
                electrical_component_category_specific_info::Kind,
            },
        },
        google::protobuf,
        microgrid::{
            AugmentElectricalComponentBoundsRequest, AugmentElectricalComponentBoundsResponse,
            ListElectricalComponentConnectionsRequest, ListElectricalComponentConnectionsResponse,
            ListElectricalComponentsRequest, ListElectricalComponentsResponse,
            ReceiveElectricalComponentTelemetryStreamRequest,
            ReceiveElectricalComponentTelemetryStreamResponse,
        },
    },
    quantity::{Current, Power, ReactivePower, Voltage},
};

/// A mock implementation of the `MicrogridApiClient` trait for testing purposes.
///
/// This mock client allows setting predefined responses for each method,
/// enabling controlled testing of components that depend on the microgrid API client.
pub struct MockMicrogridApiClient {
    pub components: Vec<Arc<MockComponent>>,
    pub connections: Vec<ElectricalComponentConnection>,
    /// Shared clock used for every emitted `sample_time`. Tests that want
    /// to inject wall-clock jumps construct their own [`TokioSyncedClock`],
    /// share a clone with [`LogicalMeterActor`], and pass another in via
    /// [`MockMicrogridApiClient::new_with_clock`].
    clock: TokioSyncedClock,
    pub augment_bounds_calls: Arc<Mutex<Vec<AugmentElectricalComponentBoundsRequest>>>,
}

/// One row per emitted telemetry frame: `(power, reactive_power, voltage,
/// current)`. Each field is independently optional so individual metrics
/// can be omitted from a frame.
pub type MockMetricRow = (
    Option<Power>,
    Option<ReactivePower>,
    Option<Voltage>,
    Option<Current>,
);

#[derive(Default, Debug, Clone)]
pub struct MockComponent {
    pub component: ElectricalComponent,
    pub children: Vec<MockComponent>,
    pub metrics: Vec<MockMetricRow>,
    /// Overrides the state code reported in each telemetry sample. `None`
    /// defaults to `Ready`.
    state_code: Option<ElectricalComponentStateCode>,
    /// When `true`, the mock stream task holds the sender open (silent)
    /// after the `metrics` vec is exhausted instead of dropping it, which
    /// prevents the client actor from reconnecting and replaying the same
    /// data. Useful for testing missing-data timeouts.
    silence_after_metrics: bool,
}

impl MockComponent {
    pub fn grid(component_id: u64) -> Self {
        Self {
            component: ElectricalComponent {
                id: component_id,
                name: format!("Grid {}", component_id),
                category: ElectricalComponentCategory::GridConnectionPoint as i32,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub fn meter(component_id: u64) -> Self {
        Self {
            component: ElectricalComponent {
                id: component_id,
                name: format!("Meter {}", component_id),
                category: ElectricalComponentCategory::Meter as i32,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub fn pv_inverter(component_id: u64) -> Self {
        Self {
            component: ElectricalComponent {
                id: component_id,
                name: format!("PV Inverter {}", component_id),
                category: ElectricalComponentCategory::Inverter as i32,
                category_specific_info: Some(ElectricalComponentCategorySpecificInfo {
                    kind: Some(Kind::Inverter(Inverter {
                        r#type: InverterType::Pv as i32,
                    })),
                }),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub fn battery_inverter(component_id: u64) -> Self {
        Self {
            component: ElectricalComponent {
                id: component_id,
                name: format!("Battery Inverter {}", component_id),
                category: ElectricalComponentCategory::Inverter as i32,
                category_specific_info: Some(ElectricalComponentCategorySpecificInfo {
                    kind: Some(Kind::Inverter(Inverter {
                        r#type: InverterType::Battery as i32,
                    })),
                }),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub fn battery(component_id: u64) -> Self {
        Self {
            component: ElectricalComponent {
                id: component_id,
                name: format!("Battery {}", component_id),
                category: ElectricalComponentCategory::Battery as i32,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub fn ev_charger(component_id: u64) -> Self {
        Self {
            component: ElectricalComponent {
                id: component_id,
                name: format!("EV Charger {}", component_id),
                category: ElectricalComponentCategory::EvCharger as i32,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub fn chp(component_id: u64) -> Self {
        Self {
            component: ElectricalComponent {
                id: component_id,
                name: format!("CHP {}", component_id),
                category: ElectricalComponentCategory::Chp as i32,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub fn with_children(mut self, children: Vec<MockComponent>) -> Self {
        if self.component.category == ElectricalComponentCategory::Unspecified as i32 {
            panic!("Cannot add children to a hidden load component");
        }
        self.children.extend(children);
        self
    }

    pub fn add_component_bounds(
        mut self,
        metric: i32,
        lower: Option<f32>,
        upper: Option<f32>,
    ) -> Self {
        self.component
            .metric_config_bounds
            .push(MetricConfigBounds {
                metric,
                config_bounds: Some(Bounds { lower, upper }),
            });
        self
    }

    pub fn with_power(mut self, power: Vec<f32>) -> Self {
        let mut metrics = self.metrics;
        for (i, p) in power.iter().enumerate() {
            if i >= metrics.len() {
                metrics.push((Some(Power::from_watts(*p)), None, None, None));
            } else {
                metrics[i].0 = Some(Power::from_watts(*p));
            }
        }
        self.metrics = metrics;
        self
    }

    pub fn with_reactive_power(mut self, reactive_power: Vec<f32>) -> Self {
        let mut metrics = self.metrics;
        for (i, rp) in reactive_power.iter().enumerate() {
            if i >= metrics.len() {
                metrics.push((
                    None,
                    Some(ReactivePower::from_volt_amperes_reactive(*rp)),
                    None,
                    None,
                ));
            } else {
                metrics[i].1 = Some(ReactivePower::from_volt_amperes_reactive(*rp));
            }
        }
        self.metrics = metrics;
        self
    }

    pub fn with_voltage(mut self, voltage: Vec<f32>) -> Self {
        let mut metrics = self.metrics;
        for (i, v) in voltage.iter().enumerate() {
            if i >= metrics.len() {
                metrics.push((None, None, Some(Voltage::from_volts(*v)), None));
            } else {
                metrics[i].2 = Some(Voltage::from_volts(*v));
            }
        }
        self.metrics = metrics;
        self
    }

    pub fn with_current(mut self, current: Vec<f32>) -> Self {
        let mut metrics = self.metrics;
        for (i, c) in current.iter().enumerate() {
            if i >= metrics.len() {
                metrics.push((None, None, None, Some(Current::from_amperes(*c))));
            } else {
                metrics[i].3 = Some(Current::from_amperes(*c));
            }
        }
        self.metrics = metrics;
        self
    }

    /// Overrides the state code reported in each telemetry sample.
    pub fn with_state(mut self, code: ElectricalComponentStateCode) -> Self {
        self.state_code = Some(code);
        self
    }

    /// Keeps the telemetry stream open and silent after the configured
    /// metrics are exhausted, so the client actor doesn't reconnect and
    /// replay the data. Useful for testing missing-data timeouts.
    pub fn with_silence_after_metrics(mut self) -> Self {
        self.silence_after_metrics = true;
        self
    }
}

impl MockMicrogridApiClient {
    /// Creates a new `MockMicrogridApiClient` with an internally-owned
    /// [`TokioSyncedClock`] anchored to the next whole-second boundary, so
    /// telemetry timestamps line up with the resampler's interval boundaries
    /// and tests get reproducible resampled values.
    pub fn new(graph: MockComponent) -> Self {
        let since_epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let next_sec_secs = since_epoch.as_secs() + 1;

        // The anchor is the wall-clock value `TokioSyncedClock` will report
        // at the current tokio instant; it doesn't have to match real time,
        // so there's no need to sleep until the boundary actually arrives.
        let anchor = chrono::DateTime::<chrono::Utc>::from_timestamp(next_sec_secs as i64, 0)
            .unwrap_or_else(chrono::Utc::now);
        Self::new_with_clock(graph, TokioSyncedClock::with_wall_anchor(anchor))
    }

    /// Returns a clone of the clock driving telemetry timestamps. Pass it
    /// to [`LogicalMeterHandle::try_new_with_clock`] so the resampler and
    /// the mock observe the same wall-clock value at every tokio instant.
    pub fn clock(&self) -> TokioSyncedClock {
        self.clock.clone()
    }

    /// Creates a `MockMicrogridApiClient` whose telemetry timestamps come
    /// from the given clock. Share a clone with [`LogicalMeterActor`] to
    /// simulate whole-machine NTP jumps that both sides observe.
    pub fn new_with_clock(graph: MockComponent, clock: TokioSyncedClock) -> Self {
        let mut this_client = Self {
            components: vec![],
            connections: vec![],
            clock,
            augment_bounds_calls: Arc::new(Mutex::new(Vec::new())),
        };

        fn traverse(node: &MockComponent, client: &mut MockMicrogridApiClient) {
            client.components.push(Arc::new(node.clone()));
            for child in &node.children {
                client.connections.push(ElectricalComponentConnection {
                    source_electrical_component_id: node.component.id,
                    destination_electrical_component_id: child.component.id,
                    operational_lifetime: None,
                });
                traverse(child, client);
            }
        }
        traverse(&graph, &mut this_client);

        this_client
    }

    /// Return a handle to captured augment bounds requests.
    pub fn augment_bounds_calls_handle(
        &self,
    ) -> Arc<Mutex<Vec<AugmentElectricalComponentBoundsRequest>>> {
        self.augment_bounds_calls.clone()
    }
}

#[async_trait::async_trait]
impl MicrogridApiClient for MockMicrogridApiClient {
    async fn list_electrical_components(
        &mut self,
        _request: impl tonic::IntoRequest<ListElectricalComponentsRequest> + Send,
    ) -> std::result::Result<tonic::Response<ListElectricalComponentsResponse>, tonic::Status> {
        let ListElectricalComponentsRequest {
            electrical_component_ids,
            electrical_component_categories,
        } = _request.into_request().into_inner();
        Ok(Response::new(ListElectricalComponentsResponse {
            electrical_components: self
                .components
                .iter()
                .filter(|c| {
                    (electrical_component_ids.is_empty()
                        || electrical_component_ids.contains(&c.component.id))
                        && (electrical_component_categories.is_empty()
                            || electrical_component_categories.contains(&c.component.category))
                })
                .map(|c| c.component.clone())
                .collect(),
        }))
    }

    async fn list_electrical_component_connections(
        &mut self,
        _request: impl tonic::IntoRequest<ListElectricalComponentConnectionsRequest> + Send,
    ) -> std::result::Result<
        tonic::Response<ListElectricalComponentConnectionsResponse>,
        tonic::Status,
    > {
        Ok(Response::new(ListElectricalComponentConnectionsResponse {
            electrical_component_connections: self.connections.clone(),
        }))
    }

    type TelemetryStream = ReceiverStream<
        std::result::Result<ReceiveElectricalComponentTelemetryStreamResponse, tonic::Status>,
    >;

    async fn receive_electrical_component_telemetry_stream(
        &mut self,
        request: impl tonic::IntoRequest<ReceiveElectricalComponentTelemetryStreamRequest> + Send,
    ) -> std::result::Result<tonic::Response<Self::TelemetryStream>, tonic::Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let comp_id = request.into_request().into_inner().electrical_component_id;

        let component = self
            .components
            .iter()
            .find(|c| c.component.id == comp_id)
            .cloned();

        if let Some(component) = component
            && !component.metrics.is_empty()
        {
            let metrics = component.metrics.clone();
            let state_code = component
                .state_code
                .unwrap_or(ElectricalComponentStateCode::Ready);
            let silence_after_metrics = component.silence_after_metrics;
            let clock = self.clock.clone();
            tokio::spawn(async move {
                let dur = std::time::Duration::from_millis(200);
                let mut interval = tokio::time::interval(dur);
                let offset = chrono::TimeDelta::from_std(dur).unwrap_or_default();

                for metrics in metrics.iter() {
                    interval.tick().await;
                    // `tokio::time::interval`'s first tick fires
                    // immediately, so `clock.wall_now()` is still the
                    // anchor here. Add one interval so the first sample
                    // is timestamped at `anchor + dur`, matching the
                    // resampler's first interval boundary.
                    let wall = clock.wall_now() + offset;
                    let sys_delta =
                        wall.signed_duration_since(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
                    let next_ts = SystemTime::UNIX_EPOCH
                        + std::time::Duration::from_nanos(
                            sys_delta.num_nanoseconds().unwrap_or(0).max(0) as u64,
                        );
                    let duration_since_epoch =
                        next_ts.duration_since(SystemTime::UNIX_EPOCH).unwrap();
                    let ts = Some(protobuf::Timestamp {
                        seconds: duration_since_epoch.as_secs() as i64,
                        nanos: duration_since_epoch.subsec_nanos() as i32,
                    });
                    let mut metric_samples = vec![];
                    if let Some(power) = metrics.0 {
                        metric_samples.push(MetricSample {
                            sample_time: ts,
                            metric: Metric::AcPowerActive as i32,
                            value: Some(MetricValueVariant {
                                metric_value_variant: Some(
                                    metric_value_variant::MetricValueVariant::SimpleMetric(
                                        SimpleMetricValue {
                                            value: power.as_watts(),
                                        },
                                    ),
                                ),
                            }),
                            bounds: vec![],
                            connection: None,
                        });
                    }
                    if let Some(reactive_power) = metrics.1 {
                        metric_samples.push(MetricSample {
                            sample_time: ts,
                            metric: Metric::AcPowerReactive as i32,
                            value: Some(MetricValueVariant {
                                metric_value_variant: Some(
                                    metric_value_variant::MetricValueVariant::SimpleMetric(
                                        SimpleMetricValue {
                                            value: reactive_power.as_volt_amperes_reactive(),
                                        },
                                    ),
                                ),
                            }),
                            bounds: vec![],
                            connection: None,
                        });
                    }
                    if let Some(voltage) = metrics.2 {
                        metric_samples.push(MetricSample {
                            sample_time: ts,
                            metric: Metric::AcVoltage as i32,
                            value: Some(MetricValueVariant {
                                metric_value_variant: Some(
                                    metric_value_variant::MetricValueVariant::SimpleMetric(
                                        SimpleMetricValue {
                                            value: voltage.as_volts(),
                                        },
                                    ),
                                ),
                            }),
                            bounds: vec![],
                            connection: None,
                        });
                    }
                    if let Some(current) = metrics.3 {
                        metric_samples.push(MetricSample {
                            sample_time: ts,
                            metric: Metric::AcCurrent as i32,
                            value: Some(MetricValueVariant {
                                metric_value_variant: Some(
                                    metric_value_variant::MetricValueVariant::SimpleMetric(
                                        SimpleMetricValue {
                                            value: current.as_amperes(),
                                        },
                                    ),
                                ),
                            }),
                            bounds: vec![],
                            connection: None,
                        });
                    }

                    let resp = ReceiveElectricalComponentTelemetryStreamResponse {
                        telemetry: Some(ElectricalComponentTelemetry {
                            electrical_component_id: comp_id,
                            metric_samples,
                            // TODO: support sending errors
                            state_snapshots: vec![ElectricalComponentStateSnapshot {
                                origin_time: ts,
                                states: vec![state_code as i32],
                                warnings: vec![],
                                errors: vec![],
                            }],
                        }),
                    };
                    if tx.send(Ok(resp)).await.is_err() {
                        break;
                    }
                }
                if silence_after_metrics {
                    // Hold the sender open indefinitely so the client
                    // actor doesn't see the stream end and reconnect.
                    let _keep_open = tx;
                    std::future::pending::<()>().await;
                }
            });
        }

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(stream))
    }

    async fn augment_electrical_component_bounds(
        &mut self,
        _request: impl tonic::IntoRequest<AugmentElectricalComponentBoundsRequest> + Send,
    ) -> std::result::Result<tonic::Response<AugmentElectricalComponentBoundsResponse>, tonic::Status>
    {
        // Capture calls for tests
        let req = _request.into_request().into_inner();
        self.augment_bounds_calls.lock().unwrap().push(req);

        Ok(Response::new(AugmentElectricalComponentBoundsResponse {
            valid_until_time: None,
        }))
    }
}

pub mod logging {
    use std::sync::{Arc, Mutex};

    /// Run the given async test function, capturing the logs emitted during
    /// its execution.
    ///
    /// Returns a tuple of the function's output and a vector of captured log
    /// messages.
    pub async fn capture_logs<F, Fut, Out>(test_fn: F) -> (Out, Vec<String>)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Out>,
    {
        let logs = Arc::new(Mutex::new(Vec::new()));
        let logs_clone = logs.clone();

        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || MockWriter {
                logs: logs_clone.clone(),
            })
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .without_time()
            .finish();

        let out = {
            let _guard = tracing::subscriber::set_default(subscriber);
            test_fn().await
        };

        (
            out,
            Arc::try_unwrap(logs)
                .expect("Failed to unwrap Arc")
                .into_inner()
                .expect("Failed to get Mutex content"),
        )
    }

    struct MockWriter {
        logs: Arc<Mutex<Vec<String>>>,
    }

    impl std::io::Write for MockWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let message = String::from_utf8_lossy(buf).trim().to_string();
            if !message.is_empty() {
                self.logs.lock().unwrap().push(message);
            }
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
