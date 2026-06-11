// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! A telemetry tracker for a pool of PV inverters.
//!
//! The tracker spawns a [`ComponentTelemetryTracker`] per inverter and emits a
//! [`PvPoolSnapshot`], partitioning the inverters into healthy and unhealthy
//! sets, whenever any inverter's telemetry or health classification changes.

use std::{
    collections::{BTreeSet, HashSet},
    time::Duration,
};

use tokio::sync::{broadcast, mpsc};

use crate::{
    MicrogridClientHandle,
    client::proto::common::microgrid::electrical_components::ElectricalComponentStateCode,
};

use super::component_partition::ComponentHealthPartition;
use super::component_telemetry_tracker::{ComponentHealthStatus, ComponentTelemetryTracker};

/// A snapshot of a PV pool's inverters, partitioned by health status and
/// annotated with the latest telemetry sample for each (see
/// [`ComponentHealthPartition`]).
#[derive(Clone, Debug, PartialEq)]
pub struct PvPoolSnapshot {
    pub inverters: ComponentHealthPartition,
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

    pub async fn run(self) {
        if self.component_ids.is_empty() {
            tracing::error!("No component IDs provided for PvPoolTelemetryTracker");
            return;
        }

        let mut inverters = ComponentHealthPartition::default();

        let (status_tx, mut status_rx) = mpsc::channel(100);
        for &inverter_id in &self.component_ids {
            let component_data_stream = match self
                .client
                .receive_electrical_component_telemetry_stream(inverter_id)
                .await
            {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::error!(
                        "Internal error opening telemetry stream for inverter {inverter_id}: {e}; PV pool telemetry tracker aborting.",
                    );
                    return;
                }
            };
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
            inverters.mark_unhealthy(inverter_id, None);
        }

        // Drop the original sender so the channel closes once every component
        // tracker finishes, which signals the main loop to stop.
        drop(status_tx);

        let mut interval = tokio::time::interval(Duration::from_millis(200));
        let mut last_sent: Option<PvPoolSnapshot> = None;

        loop {
            tokio::select! {
                maybe_status = status_rx.recv() => {
                    match maybe_status {
                        Some(ComponentHealthStatus::Healthy(id, data)) => {
                            inverters.mark_healthy(id, data);
                        }
                        Some(ComponentHealthStatus::Unhealthy(id, data)) => {
                            inverters.mark_unhealthy(id, data);
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
                    // The unchanged-skip below means a stable partition never
                    // reaches `send()`, whose failure is otherwise the only
                    // signal that every receiver has dropped. Check for that
                    // here so the tracker still shuts down instead of leaking.
                    if self.component_pool_status_tx.receiver_count() == 0 {
                        break;
                    }
                    // Skip sending if the partitioning hasn't changed. Comparing
                    // the whole partition (not field by field) means a future
                    // field can't silently escape change detection.
                    let unchanged = last_sent.as_ref().is_some_and(|s| s.inverters == inverters);
                    if unchanged {
                        continue;
                    }
                    let snapshot = PvPoolSnapshot {
                        inverters: inverters.clone(),
                    };
                    if self.component_pool_status_tx.send(snapshot.clone()).is_err() {
                        // All receivers dropped between the check above and here;
                        // a normal shutdown, recorded by the terminal log below.
                        break;
                    }
                    last_sent = Some(snapshot);
                },
            }
        }

        // Reaching here means every component tracker exited or every receiver
        // dropped — a normal shutdown, not an error.
        tracing::debug!(
            "PvPoolTelemetryTracker (component IDs {:?}) stopped: all component trackers or receivers are gone.",
            self.component_ids
        );
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

        assert!(snap.inverters.healthy.contains_key(&3));
        assert!(snap.inverters.unhealthy.is_empty());
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

        assert!(snap.inverters.healthy.contains_key(&3));
        assert!(snap.inverters.healthy.contains_key(&4));
        assert!(snap.inverters.unhealthy.is_empty());
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
            healthy.inverters.healthy.contains_key(&3),
            "expected inverter to go healthy after initial samples, got {:?}",
            healthy
        );

        // Advance well past the 10s missing-data tolerance — the component
        // tracker should fire its interval and reclassify the inverter.
        tokio::time::advance(std::time::Duration::from_secs(15)).await;
        let unhealthy = last_snapshot(&mut rx, 5).await;

        assert!(
            unhealthy.inverters.healthy.is_empty(),
            "inverter should be unhealthy after data stops, got healthy set {:?}",
            unhealthy.inverters.healthy.keys()
        );
        assert!(unhealthy.inverters.unhealthy.contains_key(&3));
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
            !snap.inverters.healthy.contains_key(&3),
            "inverter with Error state should not be in healthy set"
        );
        assert!(
            snap.inverters.unhealthy.contains_key(&3),
            "inverter with Error state should be in unhealthy set, got {:?}",
            snap
        );
    }
}
