// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! A clonable client handle for the microgrid API.
//!
//! Instructions received by this handle are sent to the microgrid client actor,
//! which owns the connection to the microgrid API service.

use chrono::TimeDelta;
use tokio::sync::{broadcast, mpsc, oneshot};
use tonic::transport::{Channel, Endpoint};

use crate::{
    Bounds, Error,
    client::MicrogridApiClient,
    client::proto::{
        common::metrics::Bounds as PbBounds,
        common::microgrid::electrical_components::{
            ElectricalComponent, ElectricalComponentCategory, ElectricalComponentConnection,
            ElectricalComponentTelemetry,
        },
        microgrid::microgrid_client::MicrogridClient,
    },
    metric::Metric,
};

use super::{
    SetPowerAccepted, SetPowerQuantity, instruction::Instruction,
    microgrid_client_actor::MicrogridClientActor,
};

/// A handle to the microgrid client connection.
///
/// This handle can be cloned as many times as needed, and each clone will share
/// the same underlying connection to the microgrid API.
#[derive(Clone)]
pub struct MicrogridClientHandle {
    instructions_tx: mpsc::Sender<Instruction>,
}

impl MicrogridClientHandle {
    /// Creates a new `MicrogridClientHandle` for the microgrid API at the
    /// specified URL.
    ///
    /// The connection is established lazily on the first RPC, so this method
    /// succeeds even when no server is reachable yet.  Per-call errors will
    /// surface from the individual RPC methods, and the actor's per-stream
    /// retry loop will keep attempting to reconnect telemetry streams that
    /// still have receivers.
    ///
    /// Returns an error only if `url` is not a valid endpoint URL.
    pub async fn try_new(url: impl Into<String>) -> Result<Self, Error> {
        let url = url.into();
        let channel = Endpoint::from_shared(url.clone())
            .map_err(|e| {
                Error::connection_failure(format!("Invalid microgrid API URL {url}: {e}"))
            })?
            .connect_lazy();
        Ok(Self::new_from_client(MicrogridClient::<Channel>::new(
            channel,
        )))
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
        electrical_component_categories: Vec<ElectricalComponentCategory>,
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
    /// [passive sign
    /// convention](https://en.wikipedia.org/wiki/Passive_sign_convention).
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

    /// Augments the overall bounds for a given metric of a given electrical
    /// component with the provided bounds.
    /// Returns the UTC time at which the provided bounds will expire and its
    /// effects will no longer be visible in the overall bounds for the
    /// given metric.
    ///
    /// The request parameters allows users to select a duration until
    /// which the bounds will stay in effect. If no duration is provided, then the
    /// bounds will be removed after a default duration of 5 seconds.
    ///
    /// The bounds give the range that the system will try to keep the
    /// metric within. If the metric goes outside of these bounds, the system will
    /// try to bring it back within the bounds.
    /// If the bounds for a metric are [\`lower_1`, `upper_1`],
    /// [`lower_2`, `upper_2`](<`lower_1`, `upper_1`],
    /// [`lower_2`, `upper_2`>), then this metric's `value` needs to comply with
    /// the constraints
    /// `lower_1 <= value <= upper_1` OR `lower_2 <= value <= upper_2`.
    ///
    /// If multiple bounds have been provided for a metric, then the
    /// overlapping bounds are merged into a single bound, and non-overlapping
    /// bounds are kept separate.
    /// E.g. if the bounds are [0, 10], [5, 15], [20, 30](<0, 10], [5, 15], [20, 30>), then the resulting
    /// bounds will be [0, 15], [20, 30](<0, 15], [20, 30>).
    ///
    /// The following diagram illustrates how bounds are applied:
    ///
    /// ```text,
    ///  lower_1  upper_1
    /// <----|========|--------|========|-------->
    ///                    lower_2  upper_2
    /// ```
    ///
    /// The bounds in this example are `[[lower_1, upper_1], [lower_2, upper_2]]`.
    /// ---- values here are considered out of range.
    /// ==== values here are considered within range.
    ///
    /// Note that for power metrics, regardless of the bounds, 0W is always
    /// allowed.
    ///
    /// The bounds stay in effect for `request_lifetime`, rounded down to whole
    /// seconds. The API allows between 5 seconds and 15 minutes, and a negative
    /// lifetime is sent as 0 seconds. `None` uses the service's default.
    pub async fn augment_electrical_component_bounds<M, I>(
        &self,
        electrical_component_id: u64,
        #[allow(unused_variables)] target_metric: M,
        bounds: Vec<I>,
        request_lifetime: Option<TimeDelta>,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, Error>
    where
        M: Metric,
        Bounds<M::QuantityType>: Into<PbBounds>,
        I: Into<Bounds<M::QuantityType>>,
    {
        let (response_tx, response_rx) = oneshot::channel();

        self.instructions_tx
            .send(Instruction::AugmentElectricalComponentBounds {
                response_tx,
                electrical_component_id,
                target_metric: M::METRIC,
                bounds: bounds.into_iter().map(|x| x.into().into()).collect(),
                request_lifetime,
            })
            .await
            .map_err(|_| Error::internal("failed to send instruction"))?;

        response_rx
            .await
            .map_err(|e| Error::internal(format!("failed to receive response: {e}")))?
    }

    /// Sets the active or reactive power of an electrical component.
    ///
    /// A [`Power`](crate::quantity::Power) sets active power, and a
    /// [`ReactivePower`](crate::quantity::ReactivePower) sets reactive power.
    ///
    /// For active power, negative values supply power towards the grid, and
    /// positive values consume power from it. For reactive power, negative
    /// values are capacitive (current leads voltage), and positive values are
    /// inductive (current lags voltage).
    ///
    /// The setpoint stays in effect for `request_lifetime`, rounded down to
    /// whole seconds. The API allows between 10 seconds and 15 minutes, and a
    /// negative lifetime is sent as 0 seconds. `None` uses the service's
    /// default, 60 seconds. After that, the component returns to its default
    /// state, unless it gets another setpoint.
    ///
    /// Returns once the API service accepts the request, with the time until
    /// which the setpoint stays in effect, and a receiver for its outcome:
    /// [`SetPowerOutcome::Success`](crate::SetPowerOutcome::Success),
    /// [`Failed`](crate::SetPowerOutcome::Failed), or
    /// [`Overridden`](crate::SetPowerOutcome::Overridden). If the service sends
    /// a final status straight away, the receiver already holds it, and
    /// [`SetPowerAccepted::valid_until`](crate::SetPowerAccepted::valid_until)
    /// is the time that response carries, which the API leaves unset for
    /// `FAILED` and `OVERRIDDEN`.
    ///
    /// Before the service accepts the request, a rejection, a gRPC error or a
    /// broken response stream, for example after a lost connection, returns an
    /// [`ErrorKind::APIServerError`](crate::ErrorKind::APIServerError). After
    /// that, such an error resolves the outcome receiver.
    ///
    /// A slow set-power call doesn't hold up other requests on the client. Two
    /// calls for the same component can reach the service in either order,
    /// unless the first has been accepted before the second starts. If the call
    /// is dropped before it returns, the setpoint may still be applied.
    pub async fn set_electrical_component_power<Q: SetPowerQuantity>(
        &self,
        electrical_component_id: u64,
        power: Q,
        request_lifetime: Option<TimeDelta>,
    ) -> Result<SetPowerAccepted, Error> {
        let (response_tx, response_rx) = oneshot::channel();

        self.instructions_tx
            .send(Instruction::SetElectricalComponentPower {
                electrical_component_id,
                power_type: Q::POWER_TYPE,
                power: power.base_value(),
                request_lifetime,
                response_tx,
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
        client::proto::common::{
            metrics::{SimpleMetricValue, metric_value_variant},
            microgrid::electrical_components::ElectricalComponentCategory,
        },
        client::test_utils::{MockComponent, MockMicrogridApiClient},
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
    async fn test_list_electrical_components_with_filters() {
        let handle = new_client_handle();

        let components = handle
            .list_electrical_components(vec![1, 2], vec![])
            .await
            .unwrap();
        let component_ids: Vec<u64> = components.iter().map(|c| c.id).collect();
        assert_eq!(component_ids, vec![1, 2]);

        let components = handle
            .list_electrical_components(
                vec![],
                vec![
                    ElectricalComponentCategory::Meter,
                    ElectricalComponentCategory::Battery,
                ],
            )
            .await
            .unwrap();
        let component_ids: Vec<u64> = components.iter().map(|c| c.id).collect();
        assert_eq!(component_ids, vec![2, 3, 5, 7]);

        let components = handle
            .list_electrical_components(
                vec![2, 3, 4],
                vec![
                    ElectricalComponentCategory::Meter,
                    ElectricalComponentCategory::Battery,
                ],
            )
            .await
            .unwrap();
        let component_ids: Vec<u64> = components.iter().map(|c| c.id).collect();
        assert_eq!(component_ids, vec![2, 3]);
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

    #[tokio::test(start_paused = true)]
    async fn test_resubscribing_after_all_receivers_dropped_restarts_stream() {
        let handle = new_client_handle();

        let mut telemetry_rx = handle
            .receive_electrical_component_telemetry_stream(2)
            .await
            .unwrap();
        telemetry_rx.recv().await.unwrap();
        drop(telemetry_rx);

        // Let the stream task notice that all receivers are gone (it checks
        // before each sample) and the actor process its Ended status, which
        // must evict the cached sender.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        // A fresh subscription must get a live stream again, not a
        // subscription on the dead cached channel.
        let mut telemetry_rx = handle
            .receive_electrical_component_telemetry_stream(2)
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), telemetry_rx.recv())
            .await
            .expect("no telemetry after resubscribing; stale stream cache?")
            .unwrap();
    }

    mod set_power {
        use std::{
            collections::BTreeSet,
            sync::{Arc, Mutex},
            time::Duration,
        };

        use chrono::TimeDelta;

        use crate::{
            Error, ErrorKind, MicrogridClientHandle, SetPowerAccepted, SetPowerOutcome,
            client::proto::microgrid::{
                PowerType, SetElectricalComponentPowerRequest,
                SetElectricalComponentPowerRequestStatus as RequestStatus,
            },
            client::test_utils::{
                MockComponent, MockMicrogridApiClient, TokioSyncedClock, wait_for_open_streams,
            },
            quantity::{Power, ReactivePower},
            wall_clock_timer::Clock as _,
        };

        /// Returns a mock with one battery inverter, component 6.
        fn mock() -> MockMicrogridApiClient {
            MockMicrogridApiClient::new(MockComponent::grid(1).with_children(vec![
                MockComponent::battery_inverter(6).with_children(vec![MockComponent::battery(7)]),
            ]))
        }

        /// Returns a handle on `mock`, the mock's clock, and the set-power
        /// requests the mock receives.
        fn handle_on(
            mock: MockMicrogridApiClient,
        ) -> (
            MicrogridClientHandle,
            TokioSyncedClock,
            Arc<Mutex<Vec<SetElectricalComponentPowerRequest>>>,
        ) {
            let clock = mock.clock();
            let calls = mock.set_power_calls_handle();
            (MicrogridClientHandle::new_from_client(mock), clock, calls)
        }

        /// Sets 100 W on component 6 through a handle on `mock`.
        async fn set_100w(mock: MockMicrogridApiClient) -> Result<SetPowerAccepted, Error> {
            let (handle, _, _) = handle_on(mock);
            handle
                .set_electrical_component_power(6, Power::from_watts(100.0), None)
                .await
        }

        #[tokio::test(start_paused = true)]
        async fn test_active_power_success() {
            let (handle, clock, calls) = handle_on(mock());
            let accepted_until = clock.wall_now() + TimeDelta::seconds(30);

            let accepted = handle
                .set_electrical_component_power(
                    6,
                    Power::from_kilowatts(-50.0),
                    Some(TimeDelta::seconds(30)),
                )
                .await
                .unwrap();

            assert_eq!(accepted.valid_until, Some(accepted_until));
            assert_eq!(
                accepted.outcome.await.unwrap(),
                SetPowerOutcome::Success {
                    valid_until: Some(accepted_until + TimeDelta::seconds(1))
                }
            );
            assert_eq!(
                *calls.lock().unwrap(),
                vec![SetElectricalComponentPowerRequest {
                    electrical_component_id: 6,
                    power_type: PowerType::Active as i32,
                    power: -50_000.0,
                    request_lifetime: Some(30),
                }]
            );
        }

        #[tokio::test]
        async fn test_reactive_power_without_lifetime() {
            let (handle, _, calls) = handle_on(mock());

            let accepted = handle
                .set_electrical_component_power(
                    6,
                    ReactivePower::from_kilovolt_amperes_reactive(2.5),
                    None,
                )
                .await
                .unwrap();

            assert!(matches!(
                accepted.outcome.await.unwrap(),
                SetPowerOutcome::Success { .. }
            ));
            assert_eq!(
                *calls.lock().unwrap(),
                vec![SetElectricalComponentPowerRequest {
                    electrical_component_id: 6,
                    power_type: PowerType::Reactive as i32,
                    power: 2_500.0,
                    request_lifetime: None,
                }]
            );
        }

        #[tokio::test]
        async fn test_negative_lifetime_is_sent_as_zero() {
            let (handle, _, calls) = handle_on(mock());

            handle
                .set_electrical_component_power(
                    6,
                    Power::from_watts(100.0),
                    Some(TimeDelta::seconds(-5)),
                )
                .await
                .unwrap();

            assert_eq!(calls.lock().unwrap()[0].request_lifetime, Some(0));
        }

        #[tokio::test(start_paused = true)]
        async fn test_final_statuses_become_outcomes() {
            // How long after now the mock's ACCEPTED and SUCCESS responses say
            // the setpoint stays in effect.
            let accepted_valid_for = TimeDelta::seconds(60);
            let success_valid_for = TimeDelta::seconds(61);
            for (statuses, returned_valid_for, outcome_valid_for) in [
                (
                    vec![RequestStatus::Accepted, RequestStatus::Success],
                    Some(accepted_valid_for),
                    Some(success_valid_for),
                ),
                (
                    vec![RequestStatus::Accepted, RequestStatus::Failed],
                    Some(accepted_valid_for),
                    None,
                ),
                (
                    vec![RequestStatus::Accepted, RequestStatus::Overridden],
                    Some(accepted_valid_for),
                    None,
                ),
                // A repeated ACCEPTED is skipped.
                (
                    vec![
                        RequestStatus::Accepted,
                        RequestStatus::Accepted,
                        RequestStatus::Failed,
                    ],
                    Some(accepted_valid_for),
                    None,
                ),
                // A final status without an initial response.
                (
                    vec![RequestStatus::Success],
                    Some(success_valid_for),
                    Some(success_valid_for),
                ),
                (vec![RequestStatus::Overridden], None, None),
            ] {
                let (handle, clock, _) =
                    handle_on(mock().with_set_power_statuses(statuses.clone()));
                let now = clock.wall_now();
                let expected_outcome = match statuses.last() {
                    Some(RequestStatus::Success) => SetPowerOutcome::Success {
                        valid_until: outcome_valid_for.map(|d| now + d),
                    },
                    Some(RequestStatus::Failed) => SetPowerOutcome::Failed,
                    Some(RequestStatus::Overridden) => SetPowerOutcome::Overridden,
                    other => unreachable!("no final status in the table: {other:?}"),
                };

                let accepted = handle
                    .set_electrical_component_power(6, Power::from_watts(100.0), None)
                    .await
                    .unwrap();

                assert_eq!(
                    accepted.valid_until,
                    returned_valid_for.map(|d| now + d),
                    "{statuses:?}"
                );
                assert_eq!(
                    accepted.outcome.await.unwrap(),
                    expected_outcome,
                    "{statuses:?}"
                );
            }
        }

        #[tokio::test]
        async fn test_not_accepted_is_an_error() {
            for mock in [
                mock().with_set_power_statuses(vec![RequestStatus::Rejected]),
                mock().with_set_power_statuses(vec![RequestStatus::Unspecified]),
                mock().with_set_power_statuses(vec![]),
                mock().with_set_power_responses(vec![Ok(99)]),
                mock().with_set_power_responses(vec![Err(tonic::Status::internal("boom"))]),
                mock().with_set_power_call_error(tonic::Status::invalid_argument("bad")),
                mock().with_set_power_call_error(tonic::Status::unavailable("down")),
            ] {
                let err = set_100w(mock).await.unwrap_err();

                assert_eq!(err.kind(), ErrorKind::APIServerError, "{err}");
            }
        }

        #[tokio::test]
        async fn test_no_final_status_is_an_error() {
            for after_accepted in [
                vec![],
                vec![Ok(RequestStatus::Rejected as i32)],
                vec![Ok(RequestStatus::Unspecified as i32)],
                vec![Ok(99)],
                vec![Err(tonic::Status::unknown("h2 protocol error"))],
            ] {
                let responses = [vec![Ok(RequestStatus::Accepted as i32)], after_accepted].concat();
                let accepted = set_100w(mock().with_set_power_responses(responses))
                    .await
                    .unwrap();

                let err = accepted.outcome.await.unwrap_err();
                assert_eq!(err.kind(), ErrorKind::APIServerError, "{err}");
            }
        }

        #[tokio::test]
        async fn test_dropping_outcome_receiver_closes_stream() {
            let mock = mock()
                .with_set_power_statuses(vec![RequestStatus::Accepted])
                .with_held_set_power_streams();
            let open = mock.open_set_power_streams();

            let accepted = set_100w(mock).await.unwrap();
            wait_for_open_streams(&open, BTreeSet::from([6]), Duration::from_millis(10), 100).await;

            drop(accepted.outcome);
            wait_for_open_streams(&open, BTreeSet::new(), Duration::from_millis(10), 100).await;
        }

        #[tokio::test]
        async fn test_dropping_call_before_first_response_closes_stream() {
            let mock = mock()
                .with_set_power_statuses(vec![])
                .with_held_set_power_streams();
            let open = mock.open_set_power_streams();
            let (handle, _, _) = handle_on(mock);

            let call = tokio::spawn(async move {
                handle
                    .set_electrical_component_power(6, Power::from_watts(100.0), None)
                    .await
            });
            wait_for_open_streams(&open, BTreeSet::from([6]), Duration::from_millis(10), 100).await;

            call.abort();
            wait_for_open_streams(&open, BTreeSet::new(), Duration::from_millis(10), 100).await;
        }

        #[tokio::test]
        async fn test_unanswered_call_does_not_block_the_client() {
            let (handle, _, calls) = handle_on(mock().with_unanswered_set_power_calls());

            let unanswered = tokio::spawn({
                let handle = handle.clone();
                async move {
                    handle
                        .set_electrical_component_power(6, Power::from_watts(100.0), None)
                        .await
                }
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                while calls.lock().unwrap().is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the set-power call should reach the mock");

            let components = tokio::time::timeout(
                Duration::from_secs(5),
                handle.list_electrical_components(vec![6], vec![]),
            )
            .await
            .expect("the client should answer while a set-power call waits");
            assert_eq!(components.unwrap().len(), 1);
            assert!(!unanswered.is_finished());
            unanswered.abort();
        }

        #[tokio::test]
        async fn test_call_dropped_before_the_client_handles_it_is_not_sent() {
            let (handle, _, calls) = handle_on(mock());

            for _ in 0..20 {
                let call = handle.set_electrical_component_power(6, Power::from_watts(100.0), None);
                let mut call = std::pin::pin!(call);
                // Queue the instruction, then give up before the client handles
                // it.
                assert!(futures::poll!(call.as_mut()).is_pending());
            }
            // The client handles instructions in order, so once this call
            // returns, it has handled the dropped ones.
            handle
                .set_electrical_component_power(7, Power::from_watts(100.0), None)
                .await
                .unwrap();

            let ids: Vec<u64> = calls
                .lock()
                .unwrap()
                .iter()
                .map(|request| request.electrical_component_id)
                .collect();
            assert_eq!(ids, vec![7]);
        }
    }
}
