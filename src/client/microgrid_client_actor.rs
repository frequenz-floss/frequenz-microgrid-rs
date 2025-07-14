// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! The microgrid client actor that handles communication with the microgrid API.

use crate::client::{instruction::Instruction, retry_tracker::RetryTracker};
use std::collections::HashMap;

use tokio::{
    select,
    sync::{broadcast, mpsc},
};
use tonic::transport::Channel;
use tracing::Instrument as _;

use crate::{
    Error,
    proto::{
        common::v1::microgrid::components::ComponentData,
        microgrid::v1::{
            ListComponentsRequest, ListConnectionsRequest, ReceiveComponentDataStreamRequest,
            ReceiveComponentDataStreamResponse, microgrid_client::MicrogridClient,
        },
    },
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
pub(super) struct MicrogridClientActor {
    url: String,
    instructions_rx: mpsc::Receiver<Instruction>,
}

impl MicrogridClientActor {
    pub(super) fn new(url: String, instructions_rx: mpsc::Receiver<Instruction>) -> Self {
        Self {
            url,
            instructions_rx,
        }
    }

    pub(super) async fn run(mut self) {
        let mut client = match MicrogridClient::<Channel>::connect(self.url).await {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("Could not connect to server: {e}");
                return;
            }
        };

        let mut component_streams: HashMap<u64, broadcast::Sender<ComponentData>> = HashMap::new();

        let (stream_stopped_tx, mut stream_stopped_rx) = mpsc::channel(50);
        let mut retry_timer = tokio::time::interval(std::time::Duration::from_secs(1));
        retry_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut components_to_retry = HashMap::new();

        loop {
            select! {
                instruction = self.instructions_rx.recv() => {
                    if let Err(e) = handle_instruction(
                        &mut client,
                        &mut component_streams,
                        instruction,
                        stream_stopped_tx.clone(),
                    ).await {
                        tracing::error!("MicrogridClientActor: Error handling instruction: {e}");
                    }
                }
                stream_status = stream_stopped_rx.recv() => {
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
                        &mut client,
                        &mut component_streams,
                        &mut components_to_retry,
                        stream_stopped_tx.clone(),
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
async fn handle_instruction(
    client: &mut MicrogridClient<Channel>,
    component_streams: &mut HashMap<u64, broadcast::Sender<ComponentData>>,
    instruction: Option<Instruction>,
    stream_stopped_tx: mpsc::Sender<StreamStatus>,
) -> Result<(), Error> {
    match instruction {
        Some(Instruction::GetComponentDataStream {
            component_id,
            response_tx,
        }) => {
            // If a stream for the given component already exists, subscribe to
            // it and return.
            if let Some(stream) = component_streams.get(&component_id) {
                response_tx
                    .send(stream.subscribe())
                    .map_err(|_| Error::internal("failed to send response"))?;
                return Ok(());
            }

            // If a stream for the given component does not exist, create a new
            // channel and start a task for streaming component data from the
            // API service into the channel.
            let (tx, rx) = broadcast::channel::<ComponentData>(100);
            component_streams.insert(component_id, tx.clone());
            start_component_data_stream(client, component_id, tx, stream_stopped_tx).await?;

            response_tx.send(rx).map_err(|_| {
                tracing::error!("failed to send response");
                Error::internal("failed to send response")
            })?;
        }
        Some(Instruction::ListComponents {
            response_tx,
            component_ids,
            categories,
        }) => {
            let components = client
                .list_components(ListComponentsRequest {
                    component_ids,
                    categories,
                })
                .await
                .map_err(|e| Error::connection_failure(format!("list_components failed: {e}")))
                .map(|r| r.into_inner().components);

            response_tx
                .send(components)
                .map_err(|_| Error::internal("failed to send response"))?;
        }
        Some(Instruction::ListConnections {
            response_tx,
            starts,
            ends,
        }) => {
            let connections = client
                .list_connections(ListConnectionsRequest { starts, ends })
                .await
                .map_err(|e| Error::connection_failure(format!("list_connections failed: {e}")))
                .map(|r| r.into_inner().connections);

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
async fn handle_retry_timer(
    client: &mut MicrogridClient<Channel>,
    component_streams: &mut HashMap<u64, broadcast::Sender<ComponentData>>,
    components_to_retry: &mut HashMap<u64, RetryTracker>,
    stream_stopped_tx: mpsc::Sender<StreamStatus>,
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
                start_component_data_stream(client, *component_id, tx, stream_stopped_tx.clone())
                    .await?;
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

/// Creates anew data stream for the given component ID and starts a task to
/// fetch data from it in a loop.
async fn start_component_data_stream(
    client: &mut MicrogridClient<Channel>,
    component_id: u64,
    tx: broadcast::Sender<ComponentData>,
    stream_stopped_tx: mpsc::Sender<StreamStatus>,
) -> Result<(), Error> {
    let stream = match client
        .receive_component_data_stream(ReceiveComponentDataStreamRequest {
            component_id,
            filter: None,
        })
        .await
    {
        Ok(s) => s.into_inner(),
        Err(e) => {
            stream_stopped_tx
                .send(StreamStatus::Failed(component_id))
                .await
                .map_err(|e| {
                    Error::connection_failure(format!(
                        "receive_component_data_stream failed for {component_id}: {e}",
                    ))
                })?;
            return Err(Error::connection_failure(format!(
                "receive_component_data_stream failed for {component_id}: {e}",
            )));
        }
    };

    stream_stopped_tx
        .send(StreamStatus::Connected(component_id))
        .await
        .map_err(|e| {
            Error::connection_failure(format!(
                "Failed to send stream recovered message for {component_id}: {e}",
            ))
        })?;

    // create a task to fetch data from the stream in a loop and put into a channel.
    tokio::spawn(
        run_component_data_stream(stream, component_id, tx, stream_stopped_tx).in_current_span(),
    );
    Ok(())
}

async fn run_component_data_stream(
    mut stream: tonic::Streaming<ReceiveComponentDataStreamResponse>,
    component_id: u64,
    tx: broadcast::Sender<ComponentData>,
    stream_stopped_tx: mpsc::Sender<StreamStatus>,
) {
    loop {
        if tx.receiver_count() == 0 {
            tracing::debug!(
                "Dropping ComponentData stream for component_id:{:?}",
                component_id
            );
            stream_stopped_tx
                .send(StreamStatus::Ended(component_id))
                .await
                .unwrap_or_else(|e| {
                    tracing::error!(
                        "Failed to send stream ended message for {:?}: {:?}",
                        component_id,
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
                    component_id,
                    e
                );
                break;
            }
        };
        let data = match message {
            Some(ReceiveComponentDataStreamResponse { data: Some(d) }) => d,
            Some(ReceiveComponentDataStreamResponse { data: None }) => {
                tracing::warn!(
                    "get_component_data stream returned empty data for {}",
                    component_id
                );
                continue;
            }
            None => {
                tracing::warn!("get_component_data stream ended for {:?}", component_id);
                break;
            }
        };

        if tx.send(data).is_err() {
            continue;
        };
    }

    if let Err(e) = stream_stopped_tx
        .send(StreamStatus::Failed(component_id))
        .await
    {
        tracing::error!(
            "Failed to send stream stopped message for {:?}: {:?}",
            component_id,
            e
        );
    }
}
