// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! A telemetry tracker for a pool of steam boilers.
//!
//! The tracker spawns a [`ComponentTelemetryTracker`] per boiler and emits a
//! [`SteamBoilerPoolSnapshot`], partitioning the boilers into healthy and unhealthy
//! sets, whenever any boiler's telemetry or health classification changes.

use std::{
    collections::{BTreeSet, HashSet},
    time::Duration,
};

use tokio::sync::mpsc;

use crate::{
    MicrogridClientHandle,
    client::proto::common::microgrid::electrical_components::ElectricalComponentStateCode,
    microgrid::caching_sender::CachingSender,
};

use super::component_partition::ComponentHealthPartition;
use super::component_telemetry_tracker::{ComponentHealthStatus, ComponentTelemetryTracker};

/// A snapshot of a steam boiler pool's boilers, partitioned by health status and
/// annotated with the latest telemetry sample for each (see
/// [`ComponentHealthPartition`]).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SteamBoilerPoolSnapshot {
    pub boilers: ComponentHealthPartition,
}

/// A tracker that watches every steam boiler in the pool and emits a
/// [`SteamBoilerPoolSnapshot`] whenever any boiler's telemetry or health
/// classification changes.
#[derive(Clone)]
pub(crate) struct SteamBoilerPoolTelemetryTracker {
    component_ids: BTreeSet<u64>,
    component_pool_status_tx: CachingSender<SteamBoilerPoolSnapshot>,
    missing_data_tolerance: Duration,
    healthy_state_codes: HashSet<ElectricalComponentStateCode>,
    client: MicrogridClientHandle,
}

impl SteamBoilerPoolTelemetryTracker {
    pub(crate) fn new(
        component_ids: BTreeSet<u64>,
        missing_data_tolerance: Duration,
        healthy_state_codes: HashSet<ElectricalComponentStateCode>,
        client: MicrogridClientHandle,
        component_pool_status_tx: CachingSender<SteamBoilerPoolSnapshot>,
    ) -> Self {
        Self {
            component_ids,
            component_pool_status_tx,
            missing_data_tolerance,
            healthy_state_codes,
            client,
        }
    }

    pub(crate) async fn run(self) {
        let mut snapshot = SteamBoilerPoolSnapshot::default();
        for &boiler_id in &self.component_ids {
            // Every boiler starts unhealthy until it reports data.
            snapshot.boilers.mark_unhealthy(boiler_id, None);
        }

        // Publish the initial partition before opening any telemetry streams, so
        // a subscriber reading before the first update sees the pool's real
        // boilers (all unhealthy until data arrives) rather than the channel's
        // empty default. For an empty pool this is the single empty snapshot.
        // A fresh subscriber gets it (or its cached copy) at once. Ignore "no
        // receivers" here — the tick loop below owns shutdown.
        let _ = self.component_pool_status_tx.publish(snapshot.clone());

        let (status_tx, mut status_rx) = mpsc::channel(100);
        for &boiler_id in &self.component_ids {
            let component_data_stream = match self
                .client
                .receive_electrical_component_telemetry_stream(boiler_id)
                .await
            {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::error!(
                        "Internal error opening telemetry stream for steam boiler {boiler_id}: {e}; steam boiler pool telemetry tracker aborting.",
                    );
                    return;
                }
            };
            let tracker = ComponentTelemetryTracker::new(
                boiler_id,
                self.missing_data_tolerance,
                self.healthy_state_codes.clone(),
                component_data_stream,
                status_tx.clone(),
            );
            // Spawn a task for each component telemetry tracker.
            tokio::spawn(async move {
                tracker.run().await;
            });
        }

        // Drop the original sender so the channel closes once every component
        // tracker finishes, ending the loop below. An empty pool spawns no
        // trackers, so keep the sender instead: `status_rx.recv()` then parks,
        // and the tick loop drives the (empty) snapshot and the receiver-count
        // shutdown check — so the task stops when its consumers go, not before.
        let _empty_pool_keepalive = if self.component_ids.is_empty() {
            Some(status_tx)
        } else {
            drop(status_tx);
            None
        };

        let mut interval = tokio::time::interval(Duration::from_millis(200));

        loop {
            tokio::select! {
                maybe_status = status_rx.recv() => {
                    match maybe_status {
                        Some(ComponentHealthStatus::Healthy(id, data)) => {
                            snapshot.boilers.mark_healthy(id, data);
                        }
                        Some(ComponentHealthStatus::Unhealthy(id, data)) => {
                            snapshot.boilers.mark_unhealthy(id, data);
                        }
                        // Every component tracker has exited and dropped its
                        // sender, so no further updates will ever arrive. The
                        // `_ = interval.tick()` arm below is a catch-all that
                        // never disables, so the `select!` `else` branch can
                        // never run; break here instead.
                        None => break,
                    }
                },
                _ = interval.tick() => {
                    // Publish only when the partition changed (compared whole, so
                    // a future field can't escape detection); either way, stop
                    // once the last consumer has dropped.
                    if !self.component_pool_status_tx.publish_if_changed(&snapshot) {
                        break;
                    }
                },
            }
        }

        // Reaching here means either every consumer dropped or every component
        // tracker exited — a normal shutdown, not an error.
        tracing::debug!(
            "SteamBoilerPoolTelemetryTracker (component IDs {:?}) stopped: all consumers or component trackers are gone.",
            self.component_ids
        );
    }
}
