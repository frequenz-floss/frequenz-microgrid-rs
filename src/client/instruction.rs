// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Instructions that can be sent to the client actor from client handles.

use tokio::sync::{broadcast, oneshot};

use crate::{
    Error,
    proto::common::v1alpha8::microgrid::electrical_components::{
        ElectricalComponent, ElectricalComponentConnection, ElectricalComponentTelemetry,
    },
};

/// Instructions that can be sent to the client actor from client handles.
#[derive(Debug)]
pub(super) enum Instruction {
    GetComponentDataStream {
        component_id: u64,
        response_tx: oneshot::Sender<broadcast::Receiver<ElectricalComponentTelemetry>>,
    },
    ListComponents {
        component_ids: Vec<u64>,
        categories: Vec<i32>,
        response_tx: oneshot::Sender<Result<Vec<ElectricalComponent>, Error>>,
    },
    ListConnections {
        starts: Vec<u64>,
        ends: Vec<u64>,
        response_tx: oneshot::Sender<Result<Vec<ElectricalComponentConnection>, Error>>,
    },
}
