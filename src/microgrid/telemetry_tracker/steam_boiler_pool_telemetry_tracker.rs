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

#[cfg(test)]
mod tests {
    use crate::client::proto::common::microgrid::electrical_components::ElectricalComponentStateCode;
    use crate::client::test_utils::MockComponent;
    use crate::microgrid::steam_boiler_pool::SteamBoilerPool;
    use crate::microgrid::test_utils::{handles, last_snapshot};

    async fn new_pool(graph: MockComponent) -> SteamBoilerPool {
        let (client, lm) = handles(graph).await;
        SteamBoilerPool::try_new(None, client, lm).unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn single_boiler_reaches_healthy_state() {
        // grid → meter → steam_boiler(3)
        let mut pool = new_pool(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                MockComponent::steam_boiler(3).with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            ]),
        ]))
        .await;

        let mut rx = pool.telemetry_snapshots();
        let snap = last_snapshot(&mut rx, 10).await;

        assert!(snap.boilers.healthy.contains_key(&3));
        assert!(snap.boilers.unhealthy.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn two_boilers_both_appear_in_snapshot() {
        // grid → meter → [steam_boiler(3), steam_boiler(4)]
        let mut pool = new_pool(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                MockComponent::steam_boiler(3).with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                MockComponent::steam_boiler(4).with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            ]),
        ]))
        .await;

        let mut rx = pool.telemetry_snapshots();
        let snap = last_snapshot(&mut rx, 10).await;

        assert!(snap.boilers.healthy.contains_key(&3));
        assert!(snap.boilers.healthy.contains_key(&4));
        assert!(snap.boilers.unhealthy.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn calling_telemetry_snapshots_twice_reuses_sender() {
        let mut pool = new_pool(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                MockComponent::steam_boiler(3).with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            ]),
        ]))
        .await;

        let mut rx1 = pool.telemetry_snapshots();
        let mut rx2 = pool.telemetry_snapshots();

        // Advance so the tracker publishes at least one snapshot.
        tokio::time::advance(std::time::Duration::from_millis(300)).await;

        let snap1 = last_snapshot(&mut rx1, 0).await;
        let snap2 = last_snapshot(&mut rx2, 0).await;
        assert_eq!(
            snap1, snap2,
            "both subscriptions should observe the same snapshot"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn boiler_becomes_unhealthy_when_data_stops() {
        // A handful of samples then silence; the stream stays open so the
        // client actor doesn't reconnect and resupply data.
        let mut pool = new_pool(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                MockComponent::steam_boiler(3)
                    .with_power(vec![0.0, 0.0, 0.0])
                    .with_silence_after_metrics(),
            ]),
        ]))
        .await;

        let mut rx = pool.telemetry_snapshots();

        // First confirm the boiler reaches a healthy state.
        let healthy = last_snapshot(&mut rx, 10).await;
        assert!(
            healthy.boilers.healthy.contains_key(&3),
            "expected boiler to go healthy after initial samples, got {:?}",
            healthy
        );

        // Advance well past the 10s missing-data tolerance — the component
        // tracker should fire its interval and reclassify the boiler.
        tokio::time::advance(std::time::Duration::from_secs(15)).await;
        let unhealthy = last_snapshot(&mut rx, 5).await;

        assert!(
            unhealthy.boilers.healthy.is_empty(),
            "boiler should be unhealthy after data stops, got healthy set {:?}",
            unhealthy.boilers.healthy.keys()
        );
        assert!(unhealthy.boilers.unhealthy.contains_key(&3));
    }

    #[tokio::test(start_paused = true)]
    async fn boiler_with_error_state_is_unhealthy() {
        // Boiler reports an Error state — it must land in the unhealthy set
        // even though samples keep arriving.
        let mut pool = new_pool(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                MockComponent::steam_boiler(3)
                    .with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
                    .with_state(ElectricalComponentStateCode::Error),
            ]),
        ]))
        .await;

        let mut rx = pool.telemetry_snapshots();
        let snap = last_snapshot(&mut rx, 10).await;

        assert!(
            !snap.boilers.healthy.contains_key(&3),
            "boiler with Error state should not be in healthy set"
        );
        assert!(
            snap.boilers.unhealthy.contains_key(&3),
            "boiler with Error state should be in unhealthy set, got {:?}",
            snap
        );
    }
}
