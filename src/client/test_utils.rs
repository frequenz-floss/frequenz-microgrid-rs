// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! A mock implementation of the MicrogridApiClient for testing.

mod tokio_synced_clock;
pub use tokio_synced_clock::TokioSyncedClock;

use std::collections::{BTreeMap, BTreeSet, HashMap};
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
                ElectricalComponentOperationalMode, ElectricalComponentStateCode,
                ElectricalComponentStateSnapshot, ElectricalComponentTelemetry, Inverter,
                InverterType, MetricConfigBounds,
                electrical_component_category_specific_info::Kind,
            },
        },
        google::protobuf,
        microgrid::{
            AugmentElectricalComponentBoundsRequest, AugmentElectricalComponentBoundsResponse,
            ListElectricalComponentConnectionsRequest, ListElectricalComponentConnectionsResponse,
            ListElectricalComponentsRequest, ListElectricalComponentsResponse,
            ReceiveElectricalComponentTelemetryStreamRequest,
            ReceiveElectricalComponentTelemetryStreamResponse, SetElectricalComponentPowerRequest,
            SetElectricalComponentPowerRequestStatus, SetElectricalComponentPowerResponse,
        },
        time,
    },
    quantity::{Current, Frequency, Power, ReactivePower, Voltage},
};

/// A mock implementation of the `MicrogridApiClient` trait for testing purposes.
///
/// This mock client allows setting predefined responses for each method,
/// enabling controlled testing of components that depend on the microgrid API client.
#[derive(Clone)]
pub struct MockMicrogridApiClient {
    pub components: Vec<Arc<MockComponent>>,
    pub connections: Vec<ElectricalComponentConnection>,
    /// Shared clock used for every emitted `sample_time`. Tests that want
    /// to inject wall-clock jumps construct their own [`TokioSyncedClock`],
    /// share a clone with [`LogicalMeterActor`], and pass another in via
    /// [`MockMicrogridApiClient::new_with_clock`].
    clock: TokioSyncedClock,
    pub augment_bounds_calls: Arc<Mutex<Vec<AugmentElectricalComponentBoundsRequest>>>,
    /// The error that the list calls fail with, if any.
    list_call_error: Option<tonic::Status>,
    /// The set-power requests received, in order.
    set_power_calls: Arc<Mutex<Vec<SetElectricalComponentPowerRequest>>>,
    /// The status, or stream error, of each response that a set-power response
    /// stream sends, in order.
    set_power_responses: Vec<Result<i32, tonic::Status>>,
    /// The error that set-power calls fail with, if any.
    set_power_call_error: Option<tonic::Status>,
    /// Whether set-power response streams stay open after their responses.
    hold_set_power_streams: bool,
    /// Whether set-power calls never return.
    leave_set_power_calls_unanswered: bool,
    /// The component ids with a held-open set-power response stream.
    open_set_power_streams: Arc<Mutex<OpenStreams>>,
    /// Component ids with a running telemetry-stream task.
    open_streams: Arc<Mutex<OpenStreams>>,
    /// Attempts to start a telemetry stream, per component id.
    start_attempts: Arc<Mutex<HashMap<u64, usize>>>,
}

/// The component ids with an open stream that a mock tracks.
///
/// Each mock keeps one for its running telemetry-stream tasks, and one for the
/// set-power response streams it holds open after
/// `with_held_set_power_streams`.
///
/// Counts the streams per component, so a reconnect that starts a new stream
/// before the previous one has ended keeps the id open.
#[derive(Debug, Default)]
pub struct OpenStreams {
    stream_counts: BTreeMap<u64, usize>,
}

impl OpenStreams {
    /// The ids with at least one tracked stream.
    pub fn ids(&self) -> BTreeSet<u64> {
        self.stream_counts.keys().copied().collect()
    }

    /// Whether `id` has a tracked stream.
    pub fn contains(&self, id: u64) -> bool {
        self.stream_counts.contains_key(&id)
    }
}

/// Counts one tracked stream for a component while it is alive.
struct OpenStreamGuard {
    streams: Arc<Mutex<OpenStreams>>,
    id: u64,
}

impl OpenStreamGuard {
    /// Counts a new tracked stream for `id`; dropping the guard counts it down.
    fn track(streams: &Arc<Mutex<OpenStreams>>, id: u64) -> Self {
        *streams.lock().unwrap().stream_counts.entry(id).or_insert(0) += 1;
        Self {
            streams: streams.clone(),
            id,
        }
    }
}

impl Drop for OpenStreamGuard {
    fn drop(&mut self) {
        if let Ok(mut streams) = self.streams.lock()
            && let Some(count) = streams.stream_counts.get_mut(&self.id)
        {
            *count -= 1;
            if *count == 0 {
                streams.stream_counts.remove(&self.id);
            }
        }
    }
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
    /// AC frequency samples, one entry per telemetry frame (parallel to
    /// `metrics`). Set via [`MockComponent::with_frequency`].
    frequency: Vec<Option<Frequency>>,
    /// Overrides the state code reported in each telemetry sample. `None`
    /// defaults to `Ready`.
    state_code: Option<ElectricalComponentStateCode>,
    /// When `true`, the mock stream task holds the sender open (silent)
    /// after the `metrics` vec is exhausted instead of dropping it, which
    /// prevents the client actor from reconnecting and replaying the same
    /// data. Useful for testing missing-data timeouts.
    silence_after_metrics: bool,
    /// How many attempts to start the telemetry stream fail before one
    /// succeeds. Set via [`MockComponent::with_failed_starts`].
    failed_starts: usize,
    /// Bounds attached to every streamed `AcPowerActive` sample. Set via
    /// [`MockComponent::add_sample_power_bounds`]; empty by default.
    sample_power_bounds: Vec<Bounds>,
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

    pub fn steam_boiler(component_id: u64) -> Self {
        Self {
            component: ElectricalComponent {
                id: component_id,
                name: format!("Steam Boiler {}", component_id),
                category: ElectricalComponentCategory::SteamBoiler as i32,
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

    pub fn with_frequency(mut self, frequency: Vec<f32>) -> Self {
        // Extend `metrics` so every frequency frame is still emitted even when
        // no other metric is set for it (frequency lives in a parallel array).
        if frequency.len() > self.metrics.len() {
            self.metrics
                .resize(frequency.len(), (None, None, None, None));
        }
        self.frequency = frequency
            .iter()
            .map(|f| Some(Frequency::from_hertz(*f)))
            .collect();
        self
    }

    /// Overrides the state code reported in each telemetry sample.
    pub fn with_state(mut self, code: ElectricalComponentStateCode) -> Self {
        self.state_code = Some(code);
        self
    }

    /// Sets the component's operational mode.
    pub fn with_operational_mode(mut self, mode: ElectricalComponentOperationalMode) -> Self {
        self.component.operational_mode = mode as i32;
        self
    }

    /// Attaches bounds `lower..upper` to every streamed active-power sample, so
    /// pool bounds trackers have something to aggregate. Call it again to
    /// attach another range.
    pub fn add_sample_power_bounds(mut self, lower: Option<f32>, upper: Option<f32>) -> Self {
        self.sample_power_bounds.push(Bounds { lower, upper });
        self
    }

    /// Keeps the telemetry stream open and silent after the configured metrics
    /// are exhausted, so the client actor doesn't reconnect and replay the
    /// data. Useful for testing missing-data timeouts.
    pub fn with_silence_after_metrics(mut self) -> Self {
        self.silence_after_metrics = true;
        self
    }

    /// Makes the first `count` attempts to start the telemetry stream fail with
    /// `Unavailable`. Later attempts succeed.
    pub fn with_failed_starts(mut self, count: usize) -> Self {
        self.failed_starts = count;
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
            list_call_error: None,
            set_power_calls: Arc::default(),
            set_power_responses: vec![
                Ok(SetElectricalComponentPowerRequestStatus::Accepted as i32),
                Ok(SetElectricalComponentPowerRequestStatus::Success as i32),
            ],
            set_power_call_error: None,
            hold_set_power_streams: false,
            leave_set_power_calls_unanswered: false,
            open_set_power_streams: Arc::default(),
            open_streams: Arc::new(Mutex::new(OpenStreams::default())),
            start_attempts: Arc::default(),
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

    /// Makes the calls that list components and connections fail with `status`.
    #[cfg(test)]
    pub(crate) fn with_list_call_error(mut self, status: tonic::Status) -> Self {
        self.list_call_error = Some(status);
        self
    }

    /// Makes every set-power response stream send `statuses`, in order, and
    /// then end.
    ///
    /// By default, each stream sends `ACCEPTED` and then `SUCCESS`. An
    /// `ACCEPTED` response carries a `valid_until_time` of the mock clock's
    /// time plus the request lifetime, or 60 seconds if the request has none. A
    /// `SUCCESS` response carries one second more, so tests can tell the two
    /// apart.
    pub fn with_set_power_statuses(
        mut self,
        statuses: Vec<SetElectricalComponentPowerRequestStatus>,
    ) -> Self {
        self.set_power_responses = statuses
            .into_iter()
            .map(|status| Ok(status as i32))
            .collect();
        self
    }

    /// Makes every set-power response stream send `responses`, in order. Each
    /// is a raw status value, or a stream error.
    ///
    /// Replaces the statuses set by `with_set_power_statuses`, and the other
    /// way round. Responses with an `ACCEPTED` or `SUCCESS` status carry the
    /// same `valid_until_time` as with `with_set_power_statuses`.
    #[cfg(test)]
    pub(crate) fn with_set_power_responses(
        mut self,
        responses: Vec<Result<i32, tonic::Status>>,
    ) -> Self {
        self.set_power_responses = responses;
        self
    }

    /// Makes every set-power call fail with `status`.
    #[cfg(test)]
    pub(crate) fn with_set_power_call_error(mut self, status: tonic::Status) -> Self {
        self.set_power_call_error = Some(status);
        self
    }

    /// Makes every set-power call record its request, then wait for ever
    /// without returning a response stream.
    #[cfg(test)]
    pub(crate) fn with_unanswered_set_power_calls(mut self) -> Self {
        self.leave_set_power_calls_unanswered = true;
        self
    }

    /// Keeps set-power response streams open after their responses, until the
    /// client drops them.
    #[cfg(test)]
    pub(crate) fn with_held_set_power_streams(mut self) -> Self {
        self.hold_set_power_streams = true;
        self
    }

    /// The component ids whose set-power response stream is held open by
    /// `with_held_set_power_streams`.
    #[cfg(test)]
    pub(crate) fn open_set_power_streams(&self) -> Arc<Mutex<OpenStreams>> {
        self.open_set_power_streams.clone()
    }

    /// Return a handle to captured set-power requests.
    pub fn set_power_calls_handle(&self) -> Arc<Mutex<Vec<SetElectricalComponentPowerRequest>>> {
        self.set_power_calls.clone()
    }

    /// The component ids whose telemetry stream is currently open.
    ///
    /// An id joins when the mock starts streaming it and leaves when its last
    /// stream task ends: when the client drops the stream, or when the
    /// component's samples run out, unless it was built with
    /// [`MockComponent::with_silence_after_metrics`]. The client drops a stream
    /// when its last receiver is gone. A reconnect that starts a new stream
    /// task before the previous one has ended keeps the id open. A component
    /// with no samples is never counted, because no stream task is started for
    /// it.
    pub fn open_telemetry_streams(&self) -> Arc<Mutex<OpenStreams>> {
        self.open_streams.clone()
    }

    /// Counts the attempts to start a telemetry stream, per component id,
    /// failed ones included.
    #[cfg(test)]
    pub(crate) fn telemetry_start_attempts(&self) -> Arc<Mutex<HashMap<u64, usize>>> {
        self.start_attempts.clone()
    }
}

/// Polls `open` (e.g. from [`MockMicrogridApiClient::open_telemetry_streams`])
/// until its ids equal `expected`. Checks up to `attempts + 1` times, sleeping
/// `step` between checks, then panics.
pub async fn wait_for_open_streams(
    open: &Arc<Mutex<OpenStreams>>,
    expected: BTreeSet<u64>,
    step: std::time::Duration,
    attempts: u32,
) {
    let mut ids = open.lock().unwrap().ids();
    for _ in 0..attempts {
        if ids == expected {
            return;
        }
        tokio::time::sleep(step).await;
        ids = open.lock().unwrap().ids();
    }
    assert_eq!(ids, expected, "open streams");
}

#[async_trait::async_trait]
impl MicrogridApiClient for MockMicrogridApiClient {
    async fn list_electrical_components(
        &mut self,
        _request: impl tonic::IntoRequest<ListElectricalComponentsRequest> + Send,
    ) -> std::result::Result<tonic::Response<ListElectricalComponentsResponse>, tonic::Status> {
        if let Some(status) = &self.list_call_error {
            return Err(status.clone());
        }
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
        if let Some(status) = &self.list_call_error {
            return Err(status.clone());
        }
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

        let failed_starts = component.as_ref().map_or(0, |c| c.failed_starts);
        let attempts = {
            let mut start_attempts = self.start_attempts.lock().unwrap();
            let attempts = start_attempts.entry(comp_id).or_default();
            *attempts += 1;
            *attempts
        };
        if attempts <= failed_starts {
            return Err(tonic::Status::unavailable(format!(
                "telemetry stream for {comp_id} failed to start"
            )));
        }

        if let Some(component) = component
            && !component.metrics.is_empty()
        {
            let metrics = component.metrics.clone();
            let frequency = component.frequency.clone();
            let state_code = component
                .state_code
                .unwrap_or(ElectricalComponentStateCode::Ready);
            let silence_after_metrics = component.silence_after_metrics;
            let sample_power_bounds = component.sample_power_bounds.clone();
            let clock = self.clock.clone();
            let guard = OpenStreamGuard::track(&self.open_streams, comp_id);
            tokio::spawn(async move {
                let _guard = guard;
                let dur = std::time::Duration::from_millis(200);
                let mut interval = tokio::time::interval(dur);
                let offset = chrono::TimeDelta::from_std(dur).unwrap_or_default();

                for (frame, metrics) in metrics.iter().enumerate() {
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
                            bounds: sample_power_bounds.clone(),
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
                    if let Some(Some(frequency)) = frequency.get(frame) {
                        metric_samples.push(MetricSample {
                            sample_time: ts,
                            metric: Metric::AcFrequency as i32,
                            value: Some(MetricValueVariant {
                                metric_value_variant: Some(
                                    metric_value_variant::MetricValueVariant::SimpleMetric(
                                        SimpleMetricValue {
                                            value: frequency.as_hertz(),
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
                    // Hold the sender open so the client actor doesn't see the
                    // stream end and reconnect. Finish when it drops the
                    // stream.
                    tx.closed().await;
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

    type SetPowerStream = futures::stream::BoxStream<
        'static,
        std::result::Result<SetElectricalComponentPowerResponse, tonic::Status>,
    >;

    async fn set_electrical_component_power(
        &mut self,
        request: impl tonic::IntoRequest<SetElectricalComponentPowerRequest> + Send,
    ) -> std::result::Result<tonic::Response<Self::SetPowerStream>, tonic::Status> {
        use futures::StreamExt as _;

        let req = request.into_request().into_inner();
        let component_id = req.electrical_component_id;
        let lifetime_secs = req.request_lifetime.unwrap_or(60);
        self.set_power_calls.lock().unwrap().push(req);
        if let Some(status) = &self.set_power_call_error {
            return Err(status.clone());
        }
        if self.leave_set_power_calls_unanswered {
            std::future::pending::<()>().await;
        }

        let valid_until = self.clock.wall_now() + chrono::TimeDelta::seconds(lifetime_secs as i64);
        let responses = self
            .set_power_responses
            .iter()
            .map(|response| {
                response.clone().map(|status| {
                    let valid_until_time =
                        match SetElectricalComponentPowerRequestStatus::try_from(status) {
                            Ok(SetElectricalComponentPowerRequestStatus::Accepted) => {
                                Some(time::datetime_to_timestamp(valid_until))
                            }
                            Ok(SetElectricalComponentPowerRequestStatus::Success) => {
                                Some(time::datetime_to_timestamp(
                                    valid_until + chrono::TimeDelta::seconds(1),
                                ))
                            }
                            Ok(
                                SetElectricalComponentPowerRequestStatus::Unspecified
                                | SetElectricalComponentPowerRequestStatus::Rejected
                                | SetElectricalComponentPowerRequestStatus::Failed
                                | SetElectricalComponentPowerRequestStatus::Overridden,
                            )
                            | Err(_) => None,
                        };
                    SetElectricalComponentPowerResponse {
                        valid_until_time,
                        status,
                    }
                })
            })
            .collect::<Vec<_>>();
        let stream = futures::stream::iter(responses);
        if !self.hold_set_power_streams {
            return Ok(Response::new(stream.boxed()));
        }

        let open = OpenStreamGuard::track(&self.open_set_power_streams, component_id);
        let held_open = futures::stream::once(async move {
            let _open = open;
            std::future::pending().await
        });
        Ok(Response::new(stream.chain(held_open).boxed()))
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
