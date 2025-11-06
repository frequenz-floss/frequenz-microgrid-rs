// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! The microgrid API client trait and its implementation for the generated gRPC
//! client.

use tonic::transport::Channel;

use crate::proto::microgrid::v1alpha18::{
    ListElectricalComponentConnectionsRequest, ListElectricalComponentConnectionsResponse,
    ListElectricalComponentsRequest, ListElectricalComponentsResponse,
    ReceiveElectricalComponentTelemetryStreamRequest,
    ReceiveElectricalComponentTelemetryStreamResponse,
};

/// A trait representing the microgrid API client.
///
/// Includes all the client methods that are used by the `MicrogridClientActor`.
#[async_trait::async_trait]
pub trait MicrogridApiClient: Send + Sync + 'static {
    async fn list_electrical_components(
        &mut self,
        request: impl tonic::IntoRequest<ListElectricalComponentsRequest> + Send,
    ) -> std::result::Result<tonic::Response<ListElectricalComponentsResponse>, tonic::Status>;

    async fn list_electrical_component_connections(
        &mut self,
        request: impl tonic::IntoRequest<ListElectricalComponentConnectionsRequest> + Send,
    ) -> std::result::Result<
        tonic::Response<ListElectricalComponentConnectionsResponse>,
        tonic::Status,
    >;

    type TelemetryStream: futures::Stream<
            Item = std::result::Result<
                ReceiveElectricalComponentTelemetryStreamResponse,
                tonic::Status,
            >,
        > + Send
        + Unpin
        + 'static;

    async fn receive_electrical_component_telemetry_stream(
        &mut self,
        request: impl tonic::IntoRequest<ReceiveElectricalComponentTelemetryStreamRequest> + Send,
    ) -> std::result::Result<tonic::Response<Self::TelemetryStream>, tonic::Status>;
}

/// Implement the MicrogridApiClient trait for the generated gRPC client.
///
/// Forwards calls to the underlying gRPC client methods, without any additional logic.
#[async_trait::async_trait]
impl MicrogridApiClient
    for crate::proto::microgrid::v1alpha18::microgrid_client::MicrogridClient<Channel>
{
    async fn list_electrical_components(
        &mut self,
        request: impl tonic::IntoRequest<ListElectricalComponentsRequest> + Send,
    ) -> std::result::Result<tonic::Response<ListElectricalComponentsResponse>, tonic::Status> {
        self.list_electrical_components(request).await
    }

    async fn list_electrical_component_connections(
        &mut self,
        request: impl tonic::IntoRequest<ListElectricalComponentConnectionsRequest> + Send,
    ) -> std::result::Result<
        tonic::Response<ListElectricalComponentConnectionsResponse>,
        tonic::Status,
    > {
        self.list_electrical_component_connections(request).await
    }

    type TelemetryStream =
        tonic::codec::Streaming<ReceiveElectricalComponentTelemetryStreamResponse>;

    async fn receive_electrical_component_telemetry_stream(
        &mut self,
        request: impl tonic::IntoRequest<ReceiveElectricalComponentTelemetryStreamRequest> + Send,
    ) -> std::result::Result<tonic::Response<Self::TelemetryStream>, tonic::Status> {
        self.receive_electrical_component_telemetry_stream(request)
            .await
    }
}
