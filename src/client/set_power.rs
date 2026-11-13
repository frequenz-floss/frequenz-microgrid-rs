// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Types for setting the power of electrical components.

use std::{
    fmt::Display,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt as _};
use tokio::{select, sync::oneshot};

use crate::{
    Error,
    client::{
        MicrogridApiClient,
        proto::{
            microgrid::{
                PowerType, SetElectricalComponentPowerRequest,
                SetElectricalComponentPowerRequestStatus as RequestStatus,
                SetElectricalComponentPowerResponse,
            },
            time,
        },
    },
    quantity::{Power, Quantity, ReactivePower},
};

/// The power type to request for a quantity.
pub(crate) trait PowerTypeOf {
    const POWER_TYPE: PowerType;
}

/// A power quantity that can be set on an electrical component.
///
/// [`Power`] sets active power, and [`ReactivePower`] sets reactive power.
#[expect(private_bounds)]
pub trait SetPowerQuantity: PowerTypeOf + Quantity + 'static {}

impl PowerTypeOf for Power {
    const POWER_TYPE: PowerType = PowerType::Active;
}

impl SetPowerQuantity for Power {}

impl PowerTypeOf for ReactivePower {
    const POWER_TYPE: PowerType = PowerType::Reactive;
}

impl SetPowerQuantity for ReactivePower {}

/// The final status of a power setpoint that the API service accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SetPowerOutcome {
    /// The setpoint was applied.
    ///
    /// It stays in effect until `valid_until`, if the service sent one, after
    /// which the component returns to its default state unless it gets another
    /// setpoint.
    Success { valid_until: Option<DateTime<Utc>> },
    /// The service failed to apply the setpoint, and the component is not at
    /// the requested power.
    Failed,
    /// Another request overrode the setpoint.
    Overridden,
}

/// A power setpoint that the API service accepted.
#[derive(Debug)]
pub struct SetPowerAccepted {
    /// The time until which the setpoint stays in effect, if the service sent
    /// one.
    pub valid_until: Option<DateTime<Utc>>,

    /// Resolves to the setpoint's outcome.
    pub outcome: SetPowerOutcomeReceiver,
}

/// Resolves to the outcome of an accepted power setpoint.
///
/// Resolves to an error if the response stream fails, ends without a final
/// status, or sends an unknown status or one that can't follow `ACCEPTED`.
///
/// Dropping it stops waiting for the outcome, and closes the response stream
/// from the API service.
#[derive(Debug)]
pub struct SetPowerOutcomeReceiver {
    rx: oneshot::Receiver<Result<SetPowerOutcome, Error>>,
}

impl Future for SetPowerOutcomeReceiver {
    type Output = Result<SetPowerOutcome, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.rx).poll(cx).map(|result| {
            result.unwrap_or_else(|_| {
                Err(Error::internal(
                    "set-power response task ended without a final status",
                ))
            })
        })
    }
}

/// Sends a set-power request, then reports its acceptance and its outcome.
///
/// Sends the acceptance, or an error, to `response_tx`. For an accepted
/// request, then waits for the final status and sends it to the outcome
/// receiver. Returns early if the receiver for `response_tx` is dropped before
/// the service answers.
pub(super) async fn send_and_relay<T: MicrogridApiClient>(
    client: T,
    request: SetElectricalComponentPowerRequest,
    mut response_tx: oneshot::Sender<Result<SetPowerAccepted, Error>>,
) {
    let component_id = request.electrical_component_id;
    let first = select! {
        first = first_response(client, request) => first,
        () = response_tx.closed() => return,
    };
    let (mut stream, first) = match first {
        Ok(first) => first,
        Err(e) => {
            // The caller may be gone, which is fine.
            let _ = response_tx.send(Err(e));
            return;
        }
    };

    match parse_status(&first, component_id) {
        Ok(RequestStatus::Accepted) => {
            let (mut outcome_tx, accepted) =
                new_accepted(response_valid_until(&first, component_id));
            if response_tx.send(Ok(accepted)).is_err() {
                return;
            }
            select! {
                outcome = final_outcome(&mut stream, component_id) => {
                    let _ = outcome_tx.send(outcome);
                }
                // Stop waiting, and close the stream, when the receiver is
                // dropped.
                () = outcome_tx.closed() => {}
            }
        }
        Ok(
            status @ (RequestStatus::Success | RequestStatus::Failed | RequestStatus::Overridden),
        ) => {
            tracing::warn!(
                component_id,
                status = status.as_str_name(),
                "Set-power response stream started with a final status, without ACCEPTED"
            );
            let valid_until = response_valid_until(&first, component_id);
            let (outcome_tx, accepted) = new_accepted(valid_until);
            let _ = outcome_tx.send(outcome_from_status(status, valid_until, component_id));
            let _ = response_tx.send(Ok(accepted));
        }
        Ok(RequestStatus::Rejected) => {
            let _ = response_tx.send(Err(set_power_error(component_id, "rejected")));
        }
        Ok(RequestStatus::Unspecified) => {
            let _ = response_tx.send(Err(set_power_error(
                component_id,
                "unexpected initial status: UNSPECIFIED",
            )));
        }
        Err(e) => {
            let _ = response_tx.send(Err(e));
        }
    }
}

/// Returns the sender for an outcome, and the accepted setpoint it belongs to.
fn new_accepted(
    valid_until: Option<DateTime<Utc>>,
) -> (
    oneshot::Sender<Result<SetPowerOutcome, Error>>,
    SetPowerAccepted,
) {
    let (outcome_tx, outcome_rx) = oneshot::channel();
    let accepted = SetPowerAccepted {
        valid_until,
        outcome: SetPowerOutcomeReceiver { rx: outcome_rx },
    };
    (outcome_tx, accepted)
}

/// Sends a set-power request, and returns the response stream with its first
/// response.
async fn first_response<T: MicrogridApiClient>(
    mut client: T,
    request: SetElectricalComponentPowerRequest,
) -> Result<(T::SetPowerStream, SetElectricalComponentPowerResponse), Error> {
    let component_id = request.electrical_component_id;
    let mut stream = client
        .set_electrical_component_power(request)
        .await
        .map_err(|status| set_power_error(component_id, status))?
        .into_inner();
    let first = next_response(&mut stream, component_id, "before the first response").await?;
    Ok((stream, first))
}

/// Waits for the final status of an accepted set-power request.
async fn final_outcome<S>(stream: &mut S, component_id: u64) -> Result<SetPowerOutcome, Error>
where
    S: Stream<Item = Result<SetElectricalComponentPowerResponse, tonic::Status>> + Unpin,
{
    loop {
        let response = next_response(stream, component_id, "without a final status").await?;
        match parse_status(&response, component_id)? {
            RequestStatus::Accepted => continue,
            status => {
                let valid_until = response_valid_until(&response, component_id);
                return outcome_from_status(status, valid_until, component_id);
            }
        }
    }
}

/// Returns the next response from a set-power stream, or an error if the stream
/// failed or ended.
///
/// `ended` completes the error message for a stream that ended, e.g. "without
/// a final status".
async fn next_response<S>(
    stream: &mut S,
    component_id: u64,
    ended: &str,
) -> Result<SetElectricalComponentPowerResponse, Error>
where
    S: Stream<Item = Result<SetElectricalComponentPowerResponse, tonic::Status>> + Unpin,
{
    match stream.next().await {
        Some(Ok(response)) => Ok(response),
        Some(Err(status)) => Err(set_power_error(component_id, status)),
        None => Err(set_power_error(
            component_id,
            format!("stream ended {ended}"),
        )),
    }
}

/// Returns the status of a set-power response, or an error for a value that the
/// protocol doesn't define.
fn parse_status(
    response: &SetElectricalComponentPowerResponse,
    component_id: u64,
) -> Result<RequestStatus, Error> {
    RequestStatus::try_from(response.status)
        .map_err(|_| set_power_error(component_id, format!("unknown status: {}", response.status)))
}

/// Converts a final set-power status, and the `valid_until` time its response
/// carries, to an outcome.
///
/// `ACCEPTED` is not final, and `REJECTED` and `UNSPECIFIED` can't follow an
/// accepted request, so they are errors here.
fn outcome_from_status(
    status: RequestStatus,
    valid_until: Option<DateTime<Utc>>,
    component_id: u64,
) -> Result<SetPowerOutcome, Error> {
    match status {
        RequestStatus::Success => Ok(SetPowerOutcome::Success { valid_until }),
        RequestStatus::Failed => Ok(SetPowerOutcome::Failed),
        RequestStatus::Overridden => Ok(SetPowerOutcome::Overridden),
        RequestStatus::Accepted | RequestStatus::Rejected | RequestStatus::Unspecified => {
            Err(set_power_error(
                component_id,
                format!("unexpected final status: {}", status.as_str_name()),
            ))
        }
    }
}

/// Returns the time until which a response says the setpoint stays in effect.
///
/// Returns `None` if the response has no time, or logs and returns `None` if
/// the time is out of range.
fn response_valid_until(
    response: &SetElectricalComponentPowerResponse,
    component_id: u64,
) -> Option<DateTime<Utc>> {
    response.valid_until_time.and_then(|t| {
        let valid_until = time::timestamp_to_datetime(t);
        if valid_until.is_none() {
            tracing::error!(
                component_id,
                timestamp = ?t,
                "Received invalid valid_until_time in SetElectricalComponentPowerResponse"
            );
        }
        valid_until
    })
}

/// Returns an API server error about a set-power request: a gRPC error, a
/// rejection, or a response that breaks the protocol.
fn set_power_error(component_id: u64, detail: impl Display) -> Error {
    Error::api_server_error(format!(
        "set_electrical_component_power for component {component_id}: {detail}"
    ))
}
