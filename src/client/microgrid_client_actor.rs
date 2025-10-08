// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! The microgrid client actor that handles communication with the microgrid API.

use crate::{
    client::{MicrogridApiClient, instruction::Instruction, retry_tracker::RetryTracker},
    proto::microgrid::v1alpha18::{
        ListElectricalComponentConnectionsRequest, ListElectricalComponentsRequest,
        ReceiveElectricalComponentTelemetryStreamRequest,
        ReceiveElectricalComponentTelemetryStreamResponse,
    },
};
use std::collections::HashMap;

use tokio::{
    select,
    sync::{broadcast, mpsc},
};
use tracing::Instrument as _;

use crate::{
    Error, proto::common::v1alpha8::microgrid::electrical_components::ElectricalComponentTelemetry,
};

enum StreamStatus {
    Failed(u64),
    Connected(u64),
    Ended(u64),
}

/// This actor owns the connection to the microgrid API and processes instructions
/// received from any connected `MicrogridClientHandle` instance.
///
/// It allows there to be multiple `MicrogridClientHandle` instances, all
/// sharing the same connection to the microgrid API.
pub(super) struct MicrogridClientActor<T> {
    client: T,
    instructions_rx: mpsc::Receiver<Instruction>,
}

impl<T: MicrogridApiClient> MicrogridClientActor<T> {
    pub(super) fn new_from_client(client: T, instructions_rx: mpsc::Receiver<Instruction>) -> Self {
        Self {
            client,
            instructions_rx,
        }
    }

    pub(super) async fn run(mut self) {
        let mut component_streams: HashMap<u64, broadcast::Sender<ElectricalComponentTelemetry>> =
            HashMap::new();

        let (stream_status_tx, mut stream_status_rx) = mpsc::channel(50);
        let mut retry_timer = tokio::time::interval(std::time::Duration::from_secs(1));
        retry_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut components_to_retry = HashMap::new();

        loop {
            select! {
                instruction = self.instructions_rx.recv() => {
                    if let Err(e) = handle_instruction(
                        &mut self.client,
                        &mut component_streams,
                        instruction,
                        stream_status_tx.clone(),
                    ).await {
                        tracing::error!("MicrogridClientActor: Error handling instruction: {e}");
                    }
                }
                stream_status = stream_status_rx.recv() => {
                    match stream_status {
                        Some(StreamStatus::Failed(component_id)) => {
                            components_to_retry.entry(component_id).or_insert_with(
                                 RetryTracker::new
                            ).mark_new_failure();
                        }
                        Some(StreamStatus::Connected(component_id)) => {
                            components_to_retry.remove(&component_id);
                        }
                        Some(StreamStatus::Ended(component_id)) => {
                            components_to_retry.remove(&component_id);
                        }
                        None => {
                            tracing::error!("MicrogridClientActor: Stream status channel closed, exiting.");
                            return;
                        }
                    }
                }
                now = retry_timer.tick() => {
                    if let Err(e) = handle_retry_timer(
                        &mut self.client,
                        &mut component_streams,
                        &mut components_to_retry,
                        stream_status_tx.clone(),
                        now,
                    ).await {
                        tracing::error!("MicrogridClientActor: Error handling retry timer: {e}");
                    }
                }
            }
        }
    }
}

/// Handles the instructions received from the `MicrogridClientHandle` instances.
async fn handle_instruction<T: MicrogridApiClient>(
    client: &mut T,
    component_streams: &mut HashMap<u64, broadcast::Sender<ElectricalComponentTelemetry>>,
    instruction: Option<Instruction>,
    stream_status_tx: mpsc::Sender<StreamStatus>,
) -> Result<(), Error> {
    match instruction {
        Some(Instruction::ReceiveElectricalComponentTelemetryStream {
            electrical_component_id,
            response_tx,
        }) => {
            // If a stream for the given component already exists, subscribe to
            // it and return.
            if let Some(stream) = component_streams.get(&electrical_component_id) {
                response_tx
                    .send(stream.subscribe())
                    .map_err(|_| Error::internal("failed to send response"))?;
                return Ok(());
            }

            // If a stream for the given electrical component does not exist,
            // create a new channel and start a task for streaming telemetry
            // from the API service into the channel.
            let (tx, rx) = broadcast::channel::<ElectricalComponentTelemetry>(100);
            component_streams.insert(electrical_component_id, tx.clone());
            start_electrical_component_telemetry_stream(
                client,
                electrical_component_id,
                tx,
                stream_status_tx,
            )
            .await;

            response_tx.send(rx).map_err(|_| {
                tracing::error!("failed to send response");
                Error::internal("failed to send response")
            })?;
        }
        Some(Instruction::ListElectricalComponents {
            response_tx,
            electrical_component_ids,
            electrical_component_categories,
        }) => {
            let components = client
                .list_electrical_components(ListElectricalComponentsRequest {
                    electrical_component_ids,
                    electrical_component_categories,
                })
                .await
                .map_err(|e| Error::connection_failure(format!("list_components failed: {e}")))
                .map(|r| r.into_inner().electrical_components);

            response_tx
                .send(components)
                .map_err(|_| Error::internal("failed to send response"))?;
        }
        Some(Instruction::ListElectricalComponentConnections {
            response_tx,
            source_electrical_component_ids,
            destination_electrical_component_ids,
        }) => {
            let connections = client
                .list_electrical_component_connections(ListElectricalComponentConnectionsRequest {
                    source_electrical_component_ids,
                    destination_electrical_component_ids,
                })
                .await
                .map_err(|e| Error::connection_failure(format!("list_connections failed: {e}")))
                .map(|r| r.into_inner().electrical_component_connections);

            response_tx
                .send(connections)
                .map_err(|_| Error::internal("failed to send response"))?;
        }
        None => {}
    }

    Ok(())
}

/// Handles the retry timer, checking if the data streams for any components
/// need to be retried and restarting their streaming tasks if necessary.
async fn handle_retry_timer<T: MicrogridApiClient>(
    client: &mut T,
    component_streams: &mut HashMap<u64, broadcast::Sender<ElectricalComponentTelemetry>>,
    components_to_retry: &mut HashMap<u64, RetryTracker>,
    stream_status_tx: mpsc::Sender<StreamStatus>,
    now: tokio::time::Instant,
) -> Result<(), Error> {
    for item in components_to_retry.iter_mut() {
        if let Some(retry_time) = item.1.next_retry_time() {
            if retry_time > now {
                continue;
            }
            item.1.mark_new_retry();
            let (component_id, _) = item;
            if let Some(tx) = component_streams.get(component_id).cloned() {
                start_electrical_component_telemetry_stream(
                    client,
                    *component_id,
                    tx,
                    stream_status_tx.clone(),
                )
                .await;
            } else {
                tracing::error!("Component stream not found for retry: {component_id}");
                return Err(Error::internal(format!(
                    "Component stream not found for retry: {component_id}"
                )));
            }
        }
    }
    Ok(())
}

/// Creates a new data stream for the given component ID and starts a task to
/// fetch data from it in a loop.
async fn start_electrical_component_telemetry_stream<T: MicrogridApiClient>(
    client: &mut T,
    electrical_component_id: u64,
    tx: broadcast::Sender<ElectricalComponentTelemetry>,
    stream_status_tx: mpsc::Sender<StreamStatus>,
) {
    let stream = match client
        .receive_electrical_component_telemetry_stream(
            ReceiveElectricalComponentTelemetryStreamRequest {
                electrical_component_id,
                filter: None,
            },
        )
        .await
    {
        Ok(s) => s.into_inner(),
        Err(e) => {
            let _ = stream_status_tx
                .send(StreamStatus::Failed(electrical_component_id))
                .await;

            tracing::debug!("Failed to start telemetry stream for {electrical_component_id}: {e}",);
            return;
        }
    };

    if let Err(e) = stream_status_tx
        .send(StreamStatus::Connected(electrical_component_id))
        .await
    {
        tracing::error!(
            "Failed to send stream connected message for {electrical_component_id}: {e}",
        );
        return;
    }

    // create a task to fetch data from the stream in a loop and put into a channel.
    tokio::spawn(
        run_electrical_component_telemetry_stream(
            stream,
            electrical_component_id,
            tx,
            stream_status_tx,
        )
        .in_current_span(),
    );
}

async fn run_electrical_component_telemetry_stream(
    mut stream: tonic::Streaming<ReceiveElectricalComponentTelemetryStreamResponse>,
    electrical_component_id: u64,
    tx: broadcast::Sender<ElectricalComponentTelemetry>,
    stream_status_tx: mpsc::Sender<StreamStatus>,
) {
    loop {
        if tx.receiver_count() == 0 {
            tracing::debug!(
                "Dropping ComponentData stream for component_id:{:?}",
                electrical_component_id
            );
            stream_status_tx
                .send(StreamStatus::Ended(electrical_component_id))
                .await
                .unwrap_or_else(|e| {
                    tracing::error!(
                        "Failed to send stream ended message for {:?}: {:?}",
                        electrical_component_id,
                        e
                    );
                });
            return;
        }
        let message = match stream.message().await {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(
                    "get_component_data stream failed for {:?}: {:?}",
                    electrical_component_id,
                    e
                );
                break;
            }
        };
        let data = match message {
            Some(ReceiveElectricalComponentTelemetryStreamResponse { telemetry: Some(d) }) => d,
            Some(ReceiveElectricalComponentTelemetryStreamResponse { telemetry: None }) => {
                tracing::warn!(
                    "get_component_data stream returned empty data for {}",
                    electrical_component_id
                );
                continue;
            }
            None => {
                tracing::warn!(
                    "get_component_data stream ended for {:?}",
                    electrical_component_id
                );
                break;
            }
        };

        if tx.send(data).is_err() {
            continue;
        };
    }

    if let Err(e) = stream_status_tx
        .send(StreamStatus::Failed(electrical_component_id))
        .await
    {
        tracing::error!(
            "Failed to send stream stopped message for {:?}: {:?}",
            electrical_component_id,
            e
        );
    }
}
