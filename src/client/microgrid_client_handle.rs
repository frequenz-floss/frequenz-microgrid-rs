// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! A clonable client handle for the microgrid API.
//!
//! Instructions received by this handle are sent to the microgrid client actor,
//! which owns the connection to the microgrid API service.

use tokio::sync::{broadcast, mpsc, oneshot};
use tonic::transport::Channel;

use crate::{
    Error,
    client::MicrogridApiClient,
    proto::{
        common::v1alpha8::microgrid::electrical_components::{
            ElectricalComponent, ElectricalComponentConnection, ElectricalComponentTelemetry,
        },
        microgrid::v1alpha18::microgrid_client::MicrogridClient,
    },
};

use super::{instruction::Instruction, microgrid_client_actor::MicrogridClientActor};

/// A handle to the microgrid client connection.
///
/// This handle can be cloned as many times as needed, and each clone will share
/// the same underlying connection to the microgrid API.
#[derive(Clone)]
pub struct MicrogridClientHandle {
    instructions_tx: mpsc::Sender<Instruction>,
}

impl MicrogridClientHandle {
    /// Creates a new `MicrogridClientHandle` that connects to the microgrid API
    /// at the specified URL.
    pub async fn try_new(url: impl Into<String>) -> Result<Self, Error> {
        let client = match MicrogridClient::<Channel>::connect(url.into()).await {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("Could not connect to server: {e}");
                return Err(Error::connection_failure(format!(
                    "Could not connect to server: {e}"
                )));
            }
        };

        Ok(Self::new_from_client(client))
    }

    pub fn new_from_client(client: impl MicrogridApiClient) -> Self {
        let (instructions_tx, instructions_rx) = mpsc::channel(100);
        tokio::spawn(MicrogridClientActor::new_from_client(client, instructions_rx).run());
        Self { instructions_tx }
    }

    /// Returns a telemetry stream from an electrical component with a given ID.
    ///
    /// When a connection to the API service is lost, reconnecting is handled
    /// automatically, and the receiver will resume receiving data from the
    /// component once the connection is re-established.
    pub async fn receive_electrical_component_telemetry_stream(
        &self,
        electrical_component_id: u64,
    ) -> Result<broadcast::Receiver<ElectricalComponentTelemetry>, Error> {
        let (response_tx, response_rx) = oneshot::channel();

        self.instructions_tx
            .send(Instruction::ReceiveElectricalComponentTelemetryStream {
                electrical_component_id,
                response_tx,
            })
            .await
            .map_err(|_| Error::internal("failed to send instruction"))?;

        response_rx
            .await
            .map_err(|e| Error::internal(format!("failed to receive response: {e}")))
    }

    /// Lists the electrical components in the local microgrid.
    ///
    /// If provided, the filters for component IDs and categories have an `AND`
    /// relationship with one another, meaning that they are applied serially,
    /// but the elements within a single filter list have an `OR` relationship with
    /// each other.
    ///
    /// For example, if `ids` = [1, 2, 3], and `categories` = [
    ///    `ComponentCategory::COMPONENT_CATEGORY_INVERTER`,
    ///    `ComponentCategory::COMPONENT_CATEGORY_BATTERY`
    /// ],
    /// then the results will consist of elements that
    /// have the IDs 1, OR 2, OR 3,
    /// AND
    /// are of the categories `ComponentCategory::COMPONENT_CATEGORY_INVERTER` OR
    /// `ComponentCategory::COMPONENT_CATEGORY_BATTERY`.
    ///
    /// If a filter list is empty, then that filter is not applied.
    pub async fn list_electrical_components(
        &self,
        electrical_component_ids: Vec<u64>,
        electrical_component_categories: Vec<i32>,
    ) -> Result<Vec<ElectricalComponent>, Error> {
        let (response_tx, response_rx) = oneshot::channel();

        self.instructions_tx
            .send(Instruction::ListElectricalComponents {
                response_tx,
                electrical_component_ids,
                electrical_component_categories,
            })
            .await
            .map_err(|_| Error::internal("failed to send instruction"))?;

        response_rx
            .await
            .map_err(|e| Error::internal(format!("failed to receive response: {e}")))?
    }

    /// Lists the connections between the electrical components in a microgrid,
    /// denoted by `(start, end)`.
    ///
    /// The direction of a connection is always away from the grid endpoint,
    /// i.e. aligned with the direction of positive current according to the
    /// passive sign convention:
    /// https://en.wikipedia.org/wiki/Passive_sign_convention
    ///
    /// If provided, the `start` and `end` filters have an `AND` relationship
    /// between each other, meaning that they are applied serially, but an `OR`
    /// relationship with other elements in the same list.  For example, if
    /// `start` = `[1, 2, 3]`, and `end` = `[4, 5, 6]`, then the result should
    /// have all the connections where
    ///
    /// * each `start` component ID is either `1`, `2`, OR `3`,
    ///   AND
    /// * each `end` component ID is either `4`, `5`, OR `6`.
    pub async fn list_electrical_component_connections(
        &self,
        source_electrical_component_ids: Vec<u64>,
        destination_electrical_component_ids: Vec<u64>,
    ) -> Result<Vec<ElectricalComponentConnection>, Error> {
        let (response_tx, response_rx) = oneshot::channel();

        self.instructions_tx
            .send(Instruction::ListElectricalComponentConnections {
                response_tx,
                source_electrical_component_ids,
                destination_electrical_component_ids,
            })
            .await
            .map_err(|_| Error::internal("failed to send instruction"))?;

        response_rx
            .await
            .map_err(|e| Error::internal(format!("failed to receive response: {e}")))?
    }
}

#[cfg(test)]
mod tests {

    use tokio::time::Instant;

    use crate::{
        MicrogridClientHandle,
        client::test_utils::{MockComponent, MockMicrogridApiClient},
        proto::common::v1alpha8::metrics::{SimpleMetricValue, metric_value_variant},
    };

    fn new_client_handle() -> MicrogridClientHandle {
        let api_client = MockMicrogridApiClient::new(
            // Grid connection point
            MockComponent::grid(1).with_children(vec![
                // Main meter
                MockComponent::meter(2)
                    .with_power(vec![4.0, 5.0, 6.0, 7.0, 7.0, 7.0])
                    .with_children(vec![
                        // PV meter
                        MockComponent::meter(3).with_children(vec![
                            // PV inverter
                            MockComponent::pv_inverter(4),
                        ]),
                        // Battery meter
                        MockComponent::meter(5).with_children(vec![
                            // Battery inverter
                            MockComponent::battery_inverter(6).with_children(vec![
                                // Battery
                                MockComponent::battery(7),
                            ]),
                        ]),
                    ]),
            ]),
        );

        MicrogridClientHandle::new_from_client(api_client)
    }

    #[tokio::test]
    async fn test_list_electrical_components() {
        let handle = new_client_handle();

        let components = handle
            .list_electrical_components(vec![], vec![])
            .await
            .unwrap();
        let component_ids: Vec<u64> = components.iter().map(|c| c.id).collect();
        assert_eq!(component_ids, vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[tokio::test]
    async fn test_list_electrical_component_connections() {
        let handle = new_client_handle();

        let connections = handle
            .list_electrical_component_connections(vec![], vec![])
            .await
            .unwrap();

        let connection_tuples: Vec<(u64, u64)> = connections
            .iter()
            .map(|c| {
                (
                    c.source_electrical_component_id,
                    c.destination_electrical_component_id,
                )
            })
            .collect();

        assert_eq!(
            connection_tuples,
            vec![(1, 2), (2, 3), (3, 4), (2, 5), (5, 6), (6, 7)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_receive_component_telemetry_stream() {
        let handle = new_client_handle();

        let start = Instant::now();
        let mut telemetry_rx = handle
            .receive_electrical_component_telemetry_stream(2)
            .await
            .unwrap();

        let mut values = vec![];
        let mut elapsed_millis = vec![];
        for _ in 0..10 {
            let telemetry = telemetry_rx.recv().await.unwrap();
            values.push(
                if let metric_value_variant::MetricValueVariant::SimpleMetric(SimpleMetricValue {
                    value,
                }) = telemetry.metric_samples[0]
                    .value
                    .as_ref()
                    .unwrap()
                    .metric_value_variant
                    .as_ref()
                    .unwrap()
                    .clone()
                {
                    value
                } else {
                    panic!("Unexpected metric value variant for live data");
                },
            );
            elapsed_millis.push(start.elapsed().as_millis());
        }

        // Check that received values are as expected
        assert_eq!(
            values,
            vec![
                4.0, 5.0, 6.0, 7.0, 7.0, 7.0,
                // repeats because the client stream closes and the actor reconnects
                4.0, 5.0, 6.0, 7.0
            ]
        );

        // Check that reconnect delays are as expected
        assert_eq!(
            elapsed_millis,
            vec![
                0, 200, 400, 600, 800, 1000,
                // reconnect delay of 3000 ms, before receiving more samples
                4000, 4200, 4400, 4600,
            ]
        );
    }
}
