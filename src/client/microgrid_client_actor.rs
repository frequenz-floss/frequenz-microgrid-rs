// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! The microgrid client actor that handles communication with the microgrid API.

use crate::client::{
    MicrogridApiClient,
    instruction::Instruction,
    proto::common::microgrid::electrical_components::ElectricalComponentTelemetry,
    proto::microgrid::{
        ListElectricalComponentConnectionsRequest, ListElectricalComponentsRequest,
        ReceiveElectricalComponentTelemetryStreamRequest,
        ReceiveElectricalComponentTelemetryStreamResponse,
    },
    proto::time,
    retry_tracker::RetryTracker,
};
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use tokio::{
    select,
    sync::{broadcast, mpsc},
};
use tracing::Instrument as _;

use crate::Error;

enum StreamStatus {
    Failed(u64),
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
                        &mut components_to_retry,
                        instruction,
                        stream_status_tx.clone(),
                    ).await {
                        tracing::error!("MicrogridClientActor: Error handling instruction: {e}");
                    }
                }
                stream_status = stream_status_rx.recv() => {
                    match stream_status {
                        Some(StreamStatus::Failed(component_id)) => {
                            mark_stream_failed(&mut components_to_retry, component_id);
                        }
                        Some(StreamStatus::Ended(component_id)) => {
                            components_to_retry.remove(&component_id);
                            // A subscriber may have arrived between the stream
                            // task's no-receivers check and this message.
                            restart_or_forget(
                                &mut self.client,
                                &mut component_streams,
                                &mut components_to_retry,
                                component_id,
                                stream_status_tx.clone(),
                            )
                            .await;
                        }
                        None => {
                            tracing::error!("MicrogridClientActor: Stream status channel closed, exiting.");
                            return;
                        }
                    }
                }
                now = retry_timer.tick() => {
                    handle_retry_timer(
                        &mut self.client,
                        &mut component_streams,
                        &mut components_to_retry,
                        stream_status_tx.clone(),
                        now,
                    ).await;
                }
            }
        }
    }
}

/// Handles the instructions received from the `MicrogridClientHandle` instances.
async fn handle_instruction<T: MicrogridApiClient>(
    client: &mut T,
    component_streams: &mut HashMap<u64, broadcast::Sender<ElectricalComponentTelemetry>>,
    components_to_retry: &mut HashMap<u64, RetryTracker>,
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
                components_to_retry,
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
                    electrical_component_categories: electrical_component_categories
                        .into_iter()
                        .map(|c| c as i32)
                        .collect(),
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
        Some(Instruction::AugmentElectricalComponentBounds {
            electrical_component_id,
            target_metric,
            bounds,
            request_lifetime,
            response_tx,
        }) => {
            let response = client
                .augment_electrical_component_bounds(
                    crate::client::proto::microgrid::AugmentElectricalComponentBoundsRequest {
                        electrical_component_id,
                        target_metric: target_metric as i32,
                        bounds,
                        request_lifetime: time::request_lifetime_secs(request_lifetime),
                    },
                )
                .await
                .map_err(|e| {
                    Error::api_server_error(format!(
                        "augment_electrical_component_bounds failed: {e}"
                    ))
                })
                .map(|r| {
                    r.into_inner()
                        .valid_until_time
                        .and_then(|t| match time::timestamp_to_datetime(t) {
                            dt @ Some(_) => dt,
                            None => {
                                tracing::error!(
                                    concat!(
                                        "Received invalid valid_until_time in ",
                                        "AugmentElectricalComponentBoundsResponse: {:?}"
                                    ),
                                    t
                                );
                                None
                            }
                        })
                });

            response_tx
                .send(response)
                .map_err(|_| Error::internal("failed to send response"))?;
        }
        None => {}
    }

    Ok(())
}

/// Handles the retry timer: restarts or forgets each component whose stream
/// retry is due, see [`restart_or_forget`].
async fn handle_retry_timer<T: MicrogridApiClient>(
    client: &mut T,
    component_streams: &mut HashMap<u64, broadcast::Sender<ElectricalComponentTelemetry>>,
    components_to_retry: &mut HashMap<u64, RetryTracker>,
    stream_status_tx: mpsc::Sender<StreamStatus>,
    now: tokio::time::Instant,
) {
    let due: Vec<u64> = components_to_retry
        .iter()
        .filter(|(_, tracker)| tracker.next_retry_time() <= now)
        .map(|(component_id, _)| *component_id)
        .collect();
    for component_id in due {
        restart_or_forget(
            client,
            component_streams,
            components_to_retry,
            component_id,
            stream_status_tx.clone(),
        )
        .await;
    }
}

/// Restarts a component's stream if its sender still has receivers.
///
/// Otherwise forgets the component: nothing would write to the cached sender
/// anymore, so the next subscription starts a new stream instead.
async fn restart_or_forget<T: MicrogridApiClient>(
    client: &mut T,
    component_streams: &mut HashMap<u64, broadcast::Sender<ElectricalComponentTelemetry>>,
    components_to_retry: &mut HashMap<u64, RetryTracker>,
    component_id: u64,
    stream_status_tx: mpsc::Sender<StreamStatus>,
) {
    match component_streams.get(&component_id) {
        Some(tx) if tx.receiver_count() > 0 => {
            let tx = tx.clone();
            start_electrical_component_telemetry_stream(
                client,
                components_to_retry,
                component_id,
                tx,
                stream_status_tx,
            )
            .await;
        }
        Some(_) => {
            tracing::debug!("Forgetting telemetry stream for {component_id}: no receivers");
            component_streams.remove(&component_id);
            components_to_retry.remove(&component_id);
        }
        None => {
            components_to_retry.remove(&component_id);
            tracing::error!("Component stream not found for restart: {component_id}");
        }
    }
}

/// Marks a new failure for a component's stream, so that the retry timer
/// restarts it.
fn mark_stream_failed(components_to_retry: &mut HashMap<u64, RetryTracker>, component_id: u64) {
    components_to_retry
        .entry(component_id)
        .or_insert_with(RetryTracker::new)
        .mark_new_failure();
}

/// Creates a new data stream for the given component ID and starts a task to
/// fetch data from it in a loop.
///
/// Marks a failure in `components_to_retry` if the stream does not start, and
/// removes the component from it if it does. The task reports to
/// `stream_status_tx` when the stream fails or ends later.
async fn start_electrical_component_telemetry_stream<T: MicrogridApiClient>(
    client: &mut T,
    components_to_retry: &mut HashMap<u64, RetryTracker>,
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
            tracing::debug!("Failed to start telemetry stream for {electrical_component_id}: {e}",);
            mark_stream_failed(components_to_retry, electrical_component_id);
            return;
        }
    };
    components_to_retry.remove(&electrical_component_id);

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

/// Runs a component's telemetry stream, then reports to the actor why it
/// stopped.
async fn run_electrical_component_telemetry_stream(
    stream: impl Stream<Item = Result<ReceiveElectricalComponentTelemetryStreamResponse, tonic::Status>>
    + Unpin,
    electrical_component_id: u64,
    tx: broadcast::Sender<ElectricalComponentTelemetry>,
    stream_status_tx: mpsc::Sender<StreamStatus>,
) {
    // `forward_telemetry` takes the stream, so the stream is closed before this
    // task waits on the status channel. The server may limit open streams, and
    // the actor must not wait for a slot that this task holds.
    let status = forward_telemetry(stream, electrical_component_id, &tx).await;
    if let Err(e) = stream_status_tx.send(status).await {
        tracing::error!("Failed to send stream status for {electrical_component_id}: {e}");
    }
}

/// Forwards telemetry from `stream` to `tx`. Returns [`StreamStatus::Ended`] as
/// soon as `tx` has no receivers, also while waiting for a message, and
/// [`StreamStatus::Failed`] if the stream stops first.
async fn forward_telemetry(
    mut stream: impl Stream<
        Item = Result<ReceiveElectricalComponentTelemetryStreamResponse, tonic::Status>,
    > + Unpin,
    electrical_component_id: u64,
    tx: &broadcast::Sender<ElectricalComponentTelemetry>,
) -> StreamStatus {
    let closed = tx.closed();
    tokio::pin!(closed);
    loop {
        let next_message = select! {
            biased;
            () = &mut closed => {
                tracing::debug!(
                    "Dropping ComponentData stream for component_id:{:?}",
                    electrical_component_id
                );
                return StreamStatus::Ended(electrical_component_id);
            }
            next = stream.next() => next,
        };
        let Some(message) = next_message else {
            tracing::error!(
                "get_component_data stream failed for {}",
                electrical_component_id,
            );
            break;
        };
        let data = match message {
            Ok(ReceiveElectricalComponentTelemetryStreamResponse { telemetry: Some(d) }) => d,
            Ok(ReceiveElectricalComponentTelemetryStreamResponse { telemetry: None }) => {
                tracing::warn!(
                    "get_component_data stream returned empty data for {}",
                    electrical_component_id
                );
                continue;
            }
            Err(e) => {
                tracing::warn!(
                    "get_component_data stream ended for {}: {:?}",
                    electrical_component_id,
                    e
                );
                break;
            }
        };

        // A send fails only when no receivers are left, and `closed` handles
        // that case.
        let _ = tx.send(data);
    }

    StreamStatus::Failed(electrical_component_id)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tokio::sync::broadcast;

    use super::{
        ElectricalComponentTelemetry, StreamStatus, run_electrical_component_telemetry_stream,
    };

    use crate::{
        MicrogridClientHandle,
        client::test_utils::{MockComponent, MockMicrogridApiClient, wait_for_open_streams},
    };

    /// Subscribes at the same time to every meter of a site with `count`
    /// meters, ids 100 and up, each sending `samples` samples before its stream
    /// ends. Returns the client too: the test must keep it alive.
    async fn subscribe_to_meters(
        count: u64,
        samples: usize,
    ) -> (
        MicrogridClientHandle,
        Vec<broadcast::Receiver<ElectricalComponentTelemetry>>,
    ) {
        let meters = (0..count)
            .map(|i| MockComponent::meter(100 + i).with_power(vec![1.0; samples]))
            .collect();
        let client = MicrogridClientHandle::new_from_client(MockMicrogridApiClient::new(
            MockComponent::grid(1)
                .with_children(vec![MockComponent::meter(2).with_children(meters)]),
        ));
        let receivers = futures::future::join_all((100..100 + count).map(|id| {
            let client = client.clone();
            async move {
                client
                    .receive_electrical_component_telemetry_stream(id)
                    .await
                    .unwrap()
            }
        }))
        .await;
        (client, receivers)
    }

    #[tokio::test(start_paused = true)]
    async fn test_many_subscribes_at_once_all_deliver() {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let (_client, receivers) = subscribe_to_meters(200, 400).await;
            for mut rx in receivers {
                rx.recv().await.unwrap();
            }
        })
        .await
        .expect("the client actor stopped handling streams");
    }

    #[tokio::test(start_paused = true)]
    async fn test_many_streams_failing_at_once_all_reconnect() {
        // Every stream ends after its samples, all at the same time, and is
        // retried: one more message per stream means it reconnected.
        let samples = 10;
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (_client, receivers) = subscribe_to_meters(100, samples).await;
            for mut rx in receivers {
                for _ in 0..=samples {
                    rx.recv().await.unwrap();
                }
            }
        })
        .await
        .expect("the client actor stopped handling streams");
    }

    #[tokio::test(start_paused = true)]
    async fn test_failed_stream_start_is_retried() {
        let client = MicrogridClientHandle::new_from_client(MockMicrogridApiClient::new(
            MockComponent::grid(1).with_children(vec![
                MockComponent::meter(2)
                    .with_power(vec![1.0; 10])
                    .with_failed_starts(2),
            ]),
        ));
        let start = tokio::time::Instant::now();
        let mut rx = client
            .receive_electrical_component_telemetry_stream(2)
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(30), rx.recv())
            .await
            .expect("the stream was not retried")
            .unwrap();
        // The retries after the two failed starts are due at 3 s and 6 s.
        assert!(start.elapsed() >= std::time::Duration::from_secs(6));
    }

    #[tokio::test(start_paused = true)]
    async fn test_failed_stream_is_not_retried_without_receivers() {
        let api = MockMicrogridApiClient::new(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2)
                .with_power(vec![1.0; 10])
                .with_failed_starts(1),
        ]));
        let attempts = api.telemetry_start_attempts();
        let client = MicrogridClientHandle::new_from_client(api);

        let rx = client
            .receive_electrical_component_telemetry_stream(2)
            .await
            .unwrap();
        drop(rx);
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        assert_eq!(
            attempts.lock().unwrap().get(&2),
            Some(&1),
            "a stream without receivers must not be retried",
        );

        // The component was forgotten, so a new subscription starts a new
        // stream.
        let mut rx = client
            .receive_electrical_component_telemetry_stream(2)
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .expect("the new subscription did not start a stream")
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_stream_task_closes_its_stream_before_it_waits_to_report() {
        // A full status channel, so the task has to wait to report.
        let (status_tx, _status_rx) = tokio::sync::mpsc::channel(1);
        status_tx.try_send(StreamStatus::Failed(0)).unwrap();
        let (data_tx, data_rx) = tokio::sync::mpsc::channel(1);
        // No receivers, so the task stops at once.
        let (tx, _) = broadcast::channel(1);

        let task = tokio::spawn(run_electrical_component_telemetry_stream(
            tokio_stream::wrappers::ReceiverStream::new(data_rx),
            2,
            tx,
            status_tx,
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), data_tx.closed())
            .await
            .expect("the stream task kept its stream open");
        assert!(!task.is_finished());
    }

    /// Opens the stream of `meter`, reads one message, drops the receiver, and
    /// waits until the mock reports no open stream.
    async fn assert_stream_closes_when_receiver_drops(meter: MockComponent) {
        let id = meter.component.id;
        let api = MockMicrogridApiClient::new(MockComponent::grid(1).with_children(vec![meter]));
        let open = api.open_telemetry_streams();
        let client = MicrogridClientHandle::new_from_client(api);

        let mut rx = client
            .receive_electrical_component_telemetry_stream(id)
            .await
            .unwrap();
        let _ = rx.recv().await.unwrap();
        assert_eq!(open.lock().unwrap().ids(), BTreeSet::from([id]));

        drop(rx);
        wait_for_open_streams(
            &open,
            BTreeSet::new(),
            std::time::Duration::from_millis(200),
            10,
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_mock_tracks_open_telemetry_streams() {
        // Enough samples to outlast the wait, so the stream can only close
        // because the receiver went away.
        assert_stream_closes_when_receiver_drops(
            MockComponent::meter(2).with_power(vec![1.0; 400]),
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_silent_component_stream_closes_when_its_receiver_drops() {
        // One sample, then silence: no message arrives after the receiver is
        // dropped, so only the dropped receiver can close the stream.
        assert_stream_closes_when_receiver_drops(
            MockComponent::meter(2)
                .with_power(vec![1.0])
                .with_silence_after_metrics(),
        )
        .await;
    }
}
