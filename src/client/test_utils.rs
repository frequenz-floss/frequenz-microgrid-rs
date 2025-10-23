// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! A mock implementation of the MicrogridApiClient for testing.

use std::sync::Arc;

use tokio_stream::wrappers::ReceiverStream;
use tonic::Response;

use crate::proto::{
    common::v1alpha8::{
        metrics::{
            Metric, MetricSample, MetricValueVariant, SimpleMetricValue, metric_value_variant,
        },
        microgrid::electrical_components::{
            ElectricalComponent, ElectricalComponentCategory,
            ElectricalComponentCategorySpecificInfo, ElectricalComponentConnection,
            ElectricalComponentStateCode, ElectricalComponentStateSnapshot,
            ElectricalComponentTelemetry, Inverter, InverterType,
            electrical_component_category_specific_info::Kind,
        },
    },
    microgrid::v1alpha18::{
        ListElectricalComponentConnectionsRequest, ListElectricalComponentConnectionsResponse,
        ListElectricalComponentsRequest, ListElectricalComponentsResponse,
        ReceiveElectricalComponentTelemetryStreamRequest,
        ReceiveElectricalComponentTelemetryStreamResponse,
    },
};

use super::MicrogridApiClient;

/// A mock implementation of the `MicrogridApiClient` trait for testing purposes.
///
/// This mock client allows setting predefined responses for each method,
/// enabling controlled testing of components that depend on the microgrid API client.
pub struct MockMicrogridApiClient {
    pub components: Vec<Arc<MockComponent>>,
    pub connections: Vec<ElectricalComponentConnection>,
}

#[derive(Default, Debug, Clone)]
pub struct MockComponent {
    pub component: ElectricalComponent,
    pub children: Vec<Arc<MockComponent>>,
    pub power: Option<Vec<f32>>,
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
        self.children.extend(children.into_iter().map(Arc::new));
        self
    }
    pub fn with_power(mut self, power: Vec<f32>) -> Self {
        self.power = Some(power);
        self
    }
}

impl MockMicrogridApiClient {
    /// Creates a new `MockMicrogridApiClient` with default successful responses.
    pub fn new(graph: MockComponent) -> Self {
        let mut this_client = Self {
            components: vec![],
            connections: vec![],
        };

        fn traverse(node: &Arc<MockComponent>, client: &mut MockMicrogridApiClient) {
            client.components.push(node.clone());
            for child in &node.children {
                client.connections.push(ElectricalComponentConnection {
                    source_electrical_component_id: node.component.id,
                    destination_electrical_component_id: child.component.id,
                    operational_lifetime: None,
                });
                traverse(child, client);
            }
        }
        traverse(&Arc::new(graph), &mut this_client);

        this_client
    }
}

#[async_trait::async_trait]
impl MicrogridApiClient for MockMicrogridApiClient {
    async fn list_electrical_components(
        &mut self,
        _request: impl tonic::IntoRequest<ListElectricalComponentsRequest> + Send,
    ) -> std::result::Result<tonic::Response<ListElectricalComponentsResponse>, tonic::Status> {
        Ok(Response::new(ListElectricalComponentsResponse {
            electrical_components: self
                .components
                .iter()
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

        if let Some(component) = component {
            if let Some(power) = component.power.clone() {
                tokio::spawn(async move {
                    let dur = std::time::Duration::from_millis(200);
                    let mut interval = tokio::time::interval(dur);
                    let mut next_ts = std::time::SystemTime::now();
                    for p in power {
                        interval.tick().await;
                        next_ts += dur;
                        let ts = Some(prost_types::Timestamp::from(next_ts));
                        let resp = ReceiveElectricalComponentTelemetryStreamResponse {
                            telemetry: Some(ElectricalComponentTelemetry {
                                electrical_component_id: comp_id,
                                metric_samples: vec![MetricSample {
                                    sample_time: ts.clone(),
                                    metric: Metric::AcPowerActive as i32,
                                    value: Some(MetricValueVariant {
                                        metric_value_variant: Some(
                                            metric_value_variant::MetricValueVariant::SimpleMetric(
                                                SimpleMetricValue { value: p },
                                            ),
                                        ),
                                    }),
                                    bounds: vec![],
                                    connection: None,
                                }],
                                // TODO: support sending errors
                                state_snapshots: vec![ElectricalComponentStateSnapshot {
                                    origin_time: ts,
                                    states: vec![ElectricalComponentStateCode::Ready as i32],
                                    warnings: vec![],
                                    errors: vec![],
                                }],
                            }),
                        };
                        if tx.send(Ok(resp)).await.is_err() {
                            break;
                        }
                    }
                });
            }
        }

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(stream))
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
