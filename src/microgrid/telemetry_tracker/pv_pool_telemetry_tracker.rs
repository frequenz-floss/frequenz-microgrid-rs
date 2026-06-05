// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! A telemetry tracker for a pool of PV inverters.
//!
//! The tracker spawns a [`ComponentTelemetryTracker`] per inverter and emits a
//! [`PvPoolSnapshot`], partitioning the inverters into healthy and unhealthy
//! sets, whenever any inverter's telemetry or health classification changes.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    time::Duration,
};

use tokio::sync::{broadcast, mpsc};

use crate::{
    Error, MicrogridClientHandle,
    client::proto::common::microgrid::electrical_components::{
        ElectricalComponentStateCode, ElectricalComponentTelemetry,
    },
};

use super::component_telemetry_tracker::{ComponentHealthStatus, ComponentTelemetryTracker};

/// A snapshot of a PV pool's inverters, partitioned by health status and
/// annotated with the latest telemetry sample for each.
///
/// `healthy_inverters` holds the most recent [`ElectricalComponentTelemetry`]
/// observed for each healthy inverter. `unhealthy_inverters` holds the last
/// telemetry observed before the inverter became unhealthy, or `None` if no
/// sample has been received yet. Consumers can use the telemetry (including
/// per-metric bounds) directly without subscribing to the raw streams again.
#[derive(Clone, Debug, PartialEq)]
pub struct PvPoolSnapshot {
    pub healthy_inverters: HashMap<u64, ElectricalComponentTelemetry>,
    pub unhealthy_inverters: HashMap<u64, Option<ElectricalComponentTelemetry>>,
}

/// A tracker that watches every PV inverter in the pool and emits a
/// [`PvPoolSnapshot`] whenever any inverter's telemetry or health
/// classification changes.
#[derive(Clone)]
pub struct PvPoolTelemetryTracker {
    component_ids: BTreeSet<u64>,
    component_pool_status_tx: broadcast::Sender<PvPoolSnapshot>,
    missing_data_tolerance: Duration,
    healthy_state_codes: HashSet<ElectricalComponentStateCode>,
    client: MicrogridClientHandle,
}

impl PvPoolTelemetryTracker {
    pub(crate) fn new(
        component_ids: BTreeSet<u64>,
        missing_data_tolerance: Duration,
        healthy_state_codes: HashSet<ElectricalComponentStateCode>,
        client: MicrogridClientHandle,
        component_pool_status_tx: broadcast::Sender<PvPoolSnapshot>,
    ) -> Self {
        Self {
            component_ids,
            component_pool_status_tx,
            missing_data_tolerance,
            healthy_state_codes,
            client,
        }
    }

    pub async fn run(self) -> Result<(), Error> {
        if self.component_ids.is_empty() {
            let e = "No component IDs provided for PvPoolTelemetryTracker".to_string();
            tracing::error!("{}", e);
            return Err(Error::component_data_error(e));
        }

        let mut healthy_inverters: HashMap<u64, ElectricalComponentTelemetry> = HashMap::new();
        let mut unhealthy_inverters: HashMap<u64, Option<ElectricalComponentTelemetry>> =
            HashMap::new();

        let (status_tx, mut status_rx) = mpsc::channel(100);
        for &inverter_id in &self.component_ids {
            let component_data_stream = self
                .client
                .receive_electrical_component_telemetry_stream(inverter_id)
                .await?;
            let tracker = ComponentTelemetryTracker::new(
                inverter_id,
                self.missing_data_tolerance,
                self.healthy_state_codes.clone(),
                component_data_stream,
                status_tx.clone(),
            );
            // Spawn a task for each component telemetry tracker.
            tokio::spawn(async move {
                tracker.run().await;
            });
            // Initially mark the inverter as unhealthy until we see data.
            unhealthy_inverters.insert(inverter_id, None);
        }

        // Drop the original sender so the channel closes once every component
        // tracker finishes, which signals the main loop to stop.
        drop(status_tx);

        let mut interval = tokio::time::interval(Duration::from_millis(200));
        let mut last_sent: Option<PvPoolSnapshot> = None;

        loop {
            tokio::select! {
                Some(status) = status_rx.recv() => {
                    match status {
                        ComponentHealthStatus::Healthy(id, data) => {
                            healthy_inverters.insert(id, data);
                            unhealthy_inverters.remove(&id);
                        }
                        ComponentHealthStatus::Unhealthy(id, data) => {
                            unhealthy_inverters.insert(id, data);
                            healthy_inverters.remove(&id);
                        }
                    }
                },
                _ = interval.tick() => {
                    // Skip sending if the partitioning hasn't changed.
                    let unchanged = last_sent.as_ref().is_some_and(|s| {
                        s.healthy_inverters == healthy_inverters
                            && s.unhealthy_inverters == unhealthy_inverters
                    });
                    if unchanged {
                        continue;
                    }
                    let snapshot = PvPoolSnapshot {
                        healthy_inverters: healthy_inverters.clone(),
                        unhealthy_inverters: unhealthy_inverters.clone(),
                    };
                    if let Err(e) = self.component_pool_status_tx.send(snapshot.clone()) {
                        tracing::error!("Failed to send PV pool snapshot: {}", e);
                        break;
                    }
                    last_sent = Some(snapshot);
                },
                else => break,
            }
        }

        let err = format!(
            "PvPoolTelemetryTracker (component IDs {:?}) stopped receiving inverter telemetry updates.",
            self.component_ids
        );
        tracing::error!("{}", err);
        Err(Error::component_data_error(err))
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::PvPoolSnapshot;
    use crate::{
        LogicalMeterConfig, LogicalMeterHandle, MicrogridClientHandle,
        client::{
            proto::common::microgrid::electrical_components::ElectricalComponentStateCode,
            test_utils::{MockComponent, MockMicrogridApiClient},
        },
        microgrid::pv_pool::PvPool,
    };

    async fn new_pool(graph: MockComponent) -> PvPool {
        let api = MockMicrogridApiClient::new(graph);
        let client = MicrogridClientHandle::new_from_client(api);
        let lm = LogicalMeterHandle::try_new(
            client.clone(),
            LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap()),
        )
        .await
        .unwrap();
        PvPool::try_new(None, client, lm).unwrap()
    }

    /// Drains `rx` for up to `steps` * 100ms of simulated time, returning the
    /// last snapshot seen.
    async fn last_snapshot(
        rx: &mut tokio::sync::broadcast::Receiver<PvPoolSnapshot>,
        steps: u32,
    ) -> PvPoolSnapshot {
        let mut last = None;
        for _ in 0..steps {
            tokio::time::advance(std::time::Duration::from_millis(100)).await;
            while let Ok(snap) = rx.try_recv() {
                last = Some(snap);
            }
        }
        last.expect("no snapshot received")
    }

    #[tokio::test(start_paused = true)]
    async fn single_inverter_reaches_healthy_state() {
        // grid → meter → pv_inverter(3)
        let mut pool = new_pool(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                MockComponent::pv_inverter(3).with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            ]),
        ]))
        .await;

        let mut rx = pool.telemetry_snapshots();
        let snap = last_snapshot(&mut rx, 10).await;

        assert!(snap.healthy_inverters.contains_key(&3));
        assert!(snap.unhealthy_inverters.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn two_inverters_both_appear_in_snapshot() {
        // grid → meter → [pv_inverter(3), pv_inverter(4)]
        let mut pool = new_pool(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                MockComponent::pv_inverter(3).with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                MockComponent::pv_inverter(4).with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            ]),
        ]))
        .await;

        let mut rx = pool.telemetry_snapshots();
        let snap = last_snapshot(&mut rx, 10).await;

        assert!(snap.healthy_inverters.contains_key(&3));
        assert!(snap.healthy_inverters.contains_key(&4));
        assert!(snap.unhealthy_inverters.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn calling_telemetry_snapshots_twice_reuses_sender() {
        let mut pool = new_pool(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                MockComponent::pv_inverter(3).with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            ]),
        ]))
        .await;

        let mut rx1 = pool.telemetry_snapshots();
        let mut rx2 = pool.telemetry_snapshots();

        // Advance so both receivers see at least one snapshot.
        tokio::time::advance(std::time::Duration::from_millis(300)).await;

        let snap1 = rx1.recv().await.unwrap();
        let snap2 = rx2.recv().await.unwrap();
        assert_eq!(
            snap1, snap2,
            "both subscriptions should observe the same snapshot"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn inverter_becomes_unhealthy_when_data_stops() {
        // A handful of samples then silence; the stream stays open so the
        // client actor doesn't reconnect and resupply data.
        let mut pool = new_pool(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                MockComponent::pv_inverter(3)
                    .with_power(vec![0.0, 0.0, 0.0])
                    .with_silence_after_metrics(),
            ]),
        ]))
        .await;

        let mut rx = pool.telemetry_snapshots();

        // First confirm the inverter reaches a healthy state.
        let healthy = last_snapshot(&mut rx, 10).await;
        assert!(
            healthy.healthy_inverters.contains_key(&3),
            "expected inverter to go healthy after initial samples, got {:?}",
            healthy
        );

        // Advance well past the 10s missing-data tolerance — the component
        // tracker should fire its interval and reclassify the inverter.
        tokio::time::advance(std::time::Duration::from_secs(15)).await;
        let unhealthy = last_snapshot(&mut rx, 5).await;

        assert!(
            unhealthy.healthy_inverters.is_empty(),
            "inverter should be unhealthy after data stops, got healthy set {:?}",
            unhealthy.healthy_inverters.keys()
        );
        assert!(unhealthy.unhealthy_inverters.contains_key(&3));
    }

    #[tokio::test(start_paused = true)]
    async fn inverter_with_bad_state_is_unhealthy() {
        // Inverter reports an Error state — it must land in the unhealthy set
        // even though samples keep arriving.
        let mut pool = new_pool(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                MockComponent::pv_inverter(3)
                    .with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
                    .with_state(ElectricalComponentStateCode::Error),
            ]),
        ]))
        .await;

        let mut rx = pool.telemetry_snapshots();
        let snap = last_snapshot(&mut rx, 10).await;

        assert!(
            !snap.healthy_inverters.contains_key(&3),
            "inverter with Error state should not be in healthy set"
        );
        assert!(
            snap.unhealthy_inverters.contains_key(&3),
            "inverter with Error state should be in unhealthy set, got {:?}",
            snap
        );
    }
}
