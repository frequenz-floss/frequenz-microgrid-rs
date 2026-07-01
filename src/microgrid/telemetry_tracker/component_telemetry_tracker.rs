// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! A tracker that watches an electrical component's telemetry stream and
//! classifies it as healthy or unhealthy based on its state codes and the
//! freshness of the samples.

use std::{collections::HashSet, time::Duration};

use tokio::{
    select,
    sync::{broadcast, mpsc},
};

use crate::client::proto::common::microgrid::electrical_components::{
    ElectricalComponentStateCode, ElectricalComponentTelemetry,
};

pub(crate) struct ComponentTelemetryTracker {
    component_id: u64,
    missing_data_tolerance: Duration,
    component_data_rx: broadcast::Receiver<ElectricalComponentTelemetry>,
    component_status_tx: mpsc::Sender<ComponentHealthStatus>,
    healthy_state_codes: HashSet<ElectricalComponentStateCode>,
}

#[derive(PartialEq, Clone, Debug)]
pub(crate) enum ComponentHealthStatus {
    Healthy(u64, ElectricalComponentTelemetry),
    Unhealthy(u64, Option<ElectricalComponentTelemetry>),
}

impl ComponentTelemetryTracker {
    pub(super) fn new(
        component_id: u64,
        missing_data_tolerance: Duration,
        healthy_state_codes: HashSet<ElectricalComponentStateCode>,
        component_data_rx: broadcast::Receiver<ElectricalComponentTelemetry>,
        component_status_tx: mpsc::Sender<ComponentHealthStatus>,
    ) -> Self {
        Self {
            component_id,
            missing_data_tolerance,
            component_data_rx,
            component_status_tx,
            healthy_state_codes,
        }
    }

    fn state_from_data(&self, data: ElectricalComponentTelemetry) -> ComponentHealthStatus {
        for state in data.state_snapshots.iter() {
            if !state.errors.is_empty() {
                return ComponentHealthStatus::Unhealthy(self.component_id, Some(data));
            }
            for state in state.states.iter() {
                let Ok(state) = ElectricalComponentStateCode::try_from(*state) else {
                    tracing::warn!(
                        "Component {} has an invalid state code: {}",
                        self.component_id,
                        state
                    );
                    return ComponentHealthStatus::Unhealthy(self.component_id, Some(data));
                };
                if !self.healthy_state_codes.contains(&state) {
                    return ComponentHealthStatus::Unhealthy(self.component_id, Some(data));
                }
            }
        }
        ComponentHealthStatus::Healthy(data.electrical_component_id, data)
    }

    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(self.missing_data_tolerance);
        loop {
            select! {
                component_data = self.component_data_rx.recv() => {
                    match component_data {
                        Ok(data) => {
                            // Reset the interval timer on receiving valid data
                            interval.reset();
                            let status = self.state_from_data(data);
                            if self.component_status_tx.send(status).await.is_err() {
                                // The pool tracker dropped its receiver; there is
                                // nothing left to report to, so stop tracking
                                // instead of looping and logging forever.
                                tracing::debug!(
                                    "Component {} telemetry tracker stopping: pool tracker dropped its receiver.",
                                    self.component_id
                                );
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            tracing::debug!(
                                "Component {} telemetry tracker stopping: telemetry stream closed.",
                                self.component_id
                            );
                            drop(self.component_status_tx);
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    // If we reach here, it means no data was received within the tolerance period
                    let status = ComponentHealthStatus::Unhealthy(self.component_id, None);
                    if self.component_status_tx.send(status).await.is_err() {
                        // The pool tracker dropped its receiver; stop tracking.
                        tracing::debug!(
                            "Component {} telemetry tracker stopping: pool tracker dropped its receiver.",
                            self.component_id
                        );
                        break;
                    }
                }
            }
        }
    }
}
