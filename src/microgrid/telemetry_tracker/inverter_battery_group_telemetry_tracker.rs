// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! A telemetry tracker for an inverter-battery group in the microgrid, which
//! consists of a set of inverters and their associated batteries, connected
//! in MxN configuration. Emits snapshots that partition the group's
//! components into healthy and unhealthy sets, each annotated with the
//! latest telemetry sample.

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use tokio::select;

use crate::{
    Error, MicrogridClientHandle,
    client::proto::common::microgrid::electrical_components::{
        ElectricalComponentStateCode, ElectricalComponentTelemetry,
    },
    microgrid::telemetry_tracker::battery_pool_telemetry_tracker::InverterBatteryGroup,
};

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
/// status and annotated with the latest telemetry sample for each component.
///
/// The `healthy_*` maps hold the most recent [`ElectricalComponentTelemetry`]
/// observed for each healthy component. The `unhealthy_*` maps hold the last
/// telemetry observed before the component became unhealthy, or `None` if no
/// sample has been received yet. Consumers can use the telemetry (including
/// per-metric bounds) directly without subscribing to the raw streams again.
#[derive(Clone, Debug, PartialEq)]
pub struct InverterBatteryGroupStatus {
    pub healthy_inverters: HashMap<u64, ElectricalComponentTelemetry>,
    pub healthy_batteries: HashMap<u64, ElectricalComponentTelemetry>,
    pub unhealthy_inverters: HashMap<u64, Option<ElectricalComponentTelemetry>>,
    pub unhealthy_batteries: HashMap<u64, Option<ElectricalComponentTelemetry>>,
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

    pub async fn run(self) -> Result<(), Error> {
        let mut healthy_inverters = HashMap::new();
        let mut unhealthy_inverters = HashMap::new();
        let mut healthy_batteries = HashMap::new();
        let mut unhealthy_batteries = HashMap::new();

        let (inverter_status_tx, mut inverter_status_rx) = tokio::sync::mpsc::channel(100);

        for &inverter_id in &self.inverter_battery_group.inverter_ids {
            let component_data_stream = self
                .client
                .receive_electrical_component_telemetry_stream(inverter_id)
                .await?;
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
            unhealthy_inverters.insert(inverter_id, None);
        }

        let (battery_status_tx, mut battery_status_rx) = tokio::sync::mpsc::channel(100);

        for &battery_id in &self.inverter_battery_group.battery_ids {
            let component_data_stream = self
                .client
                .receive_electrical_component_telemetry_stream(battery_id)
                .await?;
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
            unhealthy_batteries.insert(battery_id, None);
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
                        let e = String::from("Inverter telemetry tracker stopped receiving status updates.");
                        tracing::error!("{}", e);
                        return Err(Error::component_data_error(e));
                    };
                    match inverter_status {
                        ComponentHealthStatus::Healthy(component_id, data) => {
                            healthy_inverters.insert(component_id, data);
                            unhealthy_inverters.remove(&component_id);
                        }
                        ComponentHealthStatus::Unhealthy(component_id, data) => {
                            unhealthy_inverters.insert(component_id, data);
                            healthy_inverters.remove(&component_id);
                        }
                    }
                },
                battery_status = battery_status_rx.recv() => {
                    let Some(battery_status) =  battery_status  else {
                        let e = String::from(
                            "Battery telemetry tracker stopped receiving status updates."
                        );
                        tracing::error!("{}", e);
                        return Err(Error::component_data_error(e));
                    };
                    match battery_status {
                        ComponentHealthStatus::Healthy(component_id, data) => {
                            healthy_batteries.insert(component_id, data);
                            unhealthy_batteries.remove(&component_id);
                        }
                        ComponentHealthStatus::Unhealthy(component_id, data) => {
                            unhealthy_batteries.insert(component_id, data);
                            healthy_batteries.remove(&component_id);
                        }
                    }
                }
            }
            if let Err(e) = self
                .status_tx
                .send((
                    self.inverter_battery_group.clone(),
                    InverterBatteryGroupStatus {
                        healthy_inverters: healthy_inverters.clone(),
                        healthy_batteries: healthy_batteries.clone(),
                        unhealthy_inverters: unhealthy_inverters.clone(),
                        unhealthy_batteries: unhealthy_batteries.clone(),
                    },
                ))
                .await
            {
                tracing::error!("Failed to send inverter-battery group status: {}", e);
                return Err(Error::component_data_error(format!(
                    "Failed to send inverter-battery group status: {}",
                    e
                )));
            }
        }
    }
}
