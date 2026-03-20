// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Instructions that can be sent to the client actor from client handles.

use chrono::TimeDelta;
use tokio::sync::{broadcast, oneshot};

use crate::{
    Error,
    proto::common::{
        metrics::{Bounds, Metric},
        microgrid::electrical_components::{
            ElectricalComponent, ElectricalComponentCategory, ElectricalComponentConnection,
            ElectricalComponentTelemetry,
        },
    },
};

/// Instructions that can be sent to the client actor from client handles.
#[derive(Debug)]
pub(super) enum Instruction {
    ReceiveElectricalComponentTelemetryStream {
        electrical_component_id: u64,
        response_tx: oneshot::Sender<broadcast::Receiver<ElectricalComponentTelemetry>>,
    },
    ListElectricalComponents {
        electrical_component_ids: Vec<u64>,
        electrical_component_categories: Vec<ElectricalComponentCategory>,
        response_tx: oneshot::Sender<Result<Vec<ElectricalComponent>, Error>>,
    },
    ListElectricalComponentConnections {
        source_electrical_component_ids: Vec<u64>,
        destination_electrical_component_ids: Vec<u64>,
        response_tx: oneshot::Sender<Result<Vec<ElectricalComponentConnection>, Error>>,
    },
    AugmentElectricalComponentBounds {
        electrical_component_id: u64,
        target_metric: Metric,
        bounds: Vec<Bounds>,
        request_lifetime: Option<TimeDelta>,
        response_tx: oneshot::Sender<Result<Option<chrono::DateTime<chrono::Utc>>, Error>>,
    },
}
