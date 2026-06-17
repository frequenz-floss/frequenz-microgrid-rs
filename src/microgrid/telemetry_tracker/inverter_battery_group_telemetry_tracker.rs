// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! A telemetry tracker for an inverter-battery group in the microgrid, which
//! consists of a set of inverters and their associated batteries, connected
//! in MxN configuration. Emits snapshots that partition the group's
//! components into healthy and unhealthy sets, each annotated with the
//! latest telemetry sample.

use std::{collections::HashSet, time::Duration};

use tokio::select;

use crate::{
    MicrogridClientHandle,
    client::proto::common::microgrid::electrical_components::ElectricalComponentStateCode,
    microgrid::telemetry_tracker::battery_pool_telemetry_tracker::InverterBatteryGroup,
};

use super::component_partition::ComponentHealthPartition;
use super::component_telemetry_tracker::{ComponentHealthStatus, ComponentTelemetryTracker};

/// A telemetry tracker for an inverter-battery group, which consists of a set
/// of inverters and their associated batteries, connected in MxN
/// configuration.
///
/// On every change, the tracker emits an [`InverterBatteryGroupStatus`] which
/// partitions the group's components into healthy and unhealthy sets and
/// carries the latest [`ElectricalComponentTelemetry`] sample seen for each
/// component. Downstream consumers (e.g. the bounds tracker) can therefore
/// read both the health state and the most recent metric samples from a
/// single subscription without re-subscribing to the telemetry streams.
#[derive(Clone)]
pub(crate) struct InverterBatteryGroupTelemetryTracker {
    inverter_battery_group: InverterBatteryGroup,
    status_tx: tokio::sync::mpsc::Sender<(InverterBatteryGroup, InverterBatteryGroupStatus)>,
    missing_data_tolerance: Duration,
    healthy_state_codes: HashSet<ElectricalComponentStateCode>,
    client: MicrogridClientHandle,
}

/// A snapshot of an inverter-battery group's components, partitioned by health
/// status and annotated with the latest telemetry sample for each component
/// (see [`ComponentHealthPartition`]).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InverterBatteryGroupStatus {
    pub inverters: ComponentHealthPartition,
    pub batteries: ComponentHealthPartition,
}

impl InverterBatteryGroupTelemetryTracker {
    pub(crate) fn new(
        inverter_battery_group: InverterBatteryGroup,
        missing_data_tolerance: Duration,
        healthy_state_codes: HashSet<ElectricalComponentStateCode>,
        client: MicrogridClientHandle,
        status_tx: tokio::sync::mpsc::Sender<(InverterBatteryGroup, InverterBatteryGroupStatus)>,
    ) -> Self {
        Self {
            inverter_battery_group,
            status_tx,
            missing_data_tolerance,
            healthy_state_codes,
            client,
        }
    }

    pub async fn run(self) {
        let mut inverters = ComponentHealthPartition::default();
        let mut batteries = ComponentHealthPartition::default();

        let (inverter_status_tx, mut inverter_status_rx) = tokio::sync::mpsc::channel(100);

        for &inverter_id in &self.inverter_battery_group.inverter_ids {
            let component_data_stream = match self
                .client
                .receive_electrical_component_telemetry_stream(inverter_id)
                .await
            {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::error!(
                        "Internal error opening telemetry stream for inverter {inverter_id}: {e}; inverter-battery group tracker aborting.",
                    );
                    return;
                }
            };
            let tracker = ComponentTelemetryTracker::new(
                inverter_id,
                self.missing_data_tolerance,
                self.healthy_state_codes.clone(),
                component_data_stream,
                inverter_status_tx.clone(),
            );
            // Spawn a task for each component telemetry tracker
            tokio::spawn(async move {
                tracker.run().await;
            });
            // Initially mark the component as unhealthy until we see data.
            inverters.mark_unhealthy(inverter_id, None);
        }

        let (battery_status_tx, mut battery_status_rx) = tokio::sync::mpsc::channel(100);

        for &battery_id in &self.inverter_battery_group.battery_ids {
            let component_data_stream = match self
                .client
                .receive_electrical_component_telemetry_stream(battery_id)
                .await
            {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::error!(
                        "Internal error opening telemetry stream for battery {battery_id}: {e}; inverter-battery group tracker aborting.",
                    );
                    return;
                }
            };
            let tracker = ComponentTelemetryTracker::new(
                battery_id,
                self.missing_data_tolerance,
                self.healthy_state_codes.clone(),
                component_data_stream,
                battery_status_tx.clone(),
            );
            // Spawn a task for each component telemetry tracker
            tokio::spawn(async move {
                tracker.run().await;
            });
            // Initially mark the component as unhealthy until we see data.
            batteries.mark_unhealthy(battery_id, None);
        }

        // Drop the original senders in the main task to allow the component
        // trackers to close the channels when they finish, which will signal
        // the main loop to stop.
        drop(inverter_status_tx);
        drop(battery_status_tx);

        loop {
            select! {
                inverter_status = inverter_status_rx.recv() => {
                    let Some(inverter_status) = inverter_status else {
                        // Every inverter component tracker has exited and dropped
                        // its sender — a normal shutdown, not an error.
                        tracing::debug!(
                            "Inverter-battery group tracker (inverters {:?}) stopping: all inverter component trackers have exited.",
                            self.inverter_battery_group.inverter_ids
                        );
                        return;
                    };
                    match inverter_status {
                        ComponentHealthStatus::Healthy(component_id, data) => {
                            inverters.mark_healthy(component_id, data);
                        }
                        ComponentHealthStatus::Unhealthy(component_id, data) => {
                            inverters.mark_unhealthy(component_id, data);
                        }
                    }
                },
                battery_status = battery_status_rx.recv() => {
                    let Some(battery_status) = battery_status else {
                        // Every battery component tracker has exited and dropped
                        // its sender — a normal shutdown, not an error.
                        tracing::debug!(
                            "Inverter-battery group tracker (batteries {:?}) stopping: all battery component trackers have exited.",
                            self.inverter_battery_group.battery_ids
                        );
                        return;
                    };
                    match battery_status {
                        ComponentHealthStatus::Healthy(component_id, data) => {
                            batteries.mark_healthy(component_id, data);
                        }
                        ComponentHealthStatus::Unhealthy(component_id, data) => {
                            batteries.mark_unhealthy(component_id, data);
                        }
                    }
                }
            }
            if self
                .status_tx
                .send((
                    self.inverter_battery_group.clone(),
                    InverterBatteryGroupStatus {
                        inverters: inverters.clone(),
                        batteries: batteries.clone(),
                    },
                ))
                .await
                .is_err()
            {
                // The pool tracker dropped its receiver — a normal shutdown.
                tracing::debug!(
                    "Inverter-battery group tracker {:?} stopping: the pool tracker dropped its receiver.",
                    self.inverter_battery_group
                );
                return;
            }
        }
    }
}
