// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! A telemetry tracker for a pool of batteries and their associated inverters.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    time::Duration,
};

use crate::{
    Error, LogicalMeterHandle, MicrogridClientHandle,
    client::proto::common::microgrid::electrical_components::ElectricalComponentStateCode,
    microgrid::telemetry_tracker::inverter_battery_group_telemetry_tracker::{
        InverterBatteryGroupStatus, InverterBatteryGroupTelemetryTracker,
    },
};

/// A set of inverters and batteries wired together in an `MxN` configuration:
/// M inverters in parallel on the AC side, N batteries in parallel on the DC
/// side, with the inverter side in series with the battery side.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct InverterBatteryGroup {
    pub inverter_ids: BTreeSet<u64>,
    pub battery_ids: BTreeSet<u64>,
}

impl InverterBatteryGroup {
    pub(crate) fn new(inverter_ids: BTreeSet<u64>, battery_ids: BTreeSet<u64>) -> Self {
        Self {
            inverter_ids,
            battery_ids,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BatteryPoolSnapshot(HashMap<InverterBatteryGroup, InverterBatteryGroupStatus>);

impl BatteryPoolSnapshot {
    pub fn groups(&self) -> &HashMap<InverterBatteryGroup, InverterBatteryGroupStatus> {
        &self.0
    }
}

/// A tracker that watches every inverter-battery group in the pool and emits
/// a [`BatteryPoolSnapshot`] whenever any component's telemetry or health
/// classification changes.
#[derive(Clone)]
pub struct BatteryPoolTelemetryTracker {
    component_ids: BTreeSet<u64>,
    component_pool_status_tx: tokio::sync::broadcast::Sender<BatteryPoolSnapshot>,
    missing_data_tolerance: Duration,
    healthy_state_codes: HashSet<ElectricalComponentStateCode>,
    client: MicrogridClientHandle,
    logical_meter: LogicalMeterHandle,
}

impl BatteryPoolTelemetryTracker {
    pub(crate) fn new(
        component_ids: BTreeSet<u64>,
        missing_data_tolerance: Duration,
        healthy_state_codes: HashSet<ElectricalComponentStateCode>,
        client: MicrogridClientHandle,
        logical_meter: LogicalMeterHandle,
        component_pool_status_tx: tokio::sync::broadcast::Sender<BatteryPoolSnapshot>,
    ) -> Self {
        Self {
            component_ids,
            component_pool_status_tx,
            missing_data_tolerance,
            healthy_state_codes,
            client,
            logical_meter,
        }
    }

    pub(crate) fn get_inverter_battery_groups(&self) -> Result<Vec<InverterBatteryGroup>, Error> {
        if self.component_ids.is_empty() {
            let e = "No component IDs provided for BatteryPoolTelemetryTracker".to_string();
            tracing::error!("{}", e);
            return Err(Error::component_data_error(e));
        }
        let mut unvisited_batteries = self.component_ids.clone();
        let mut groups = Vec::new();

        let graph = self.logical_meter.graph();

        while let Some(battery_id) = unvisited_batteries.iter().next().cloned() {
            let group_inverters = graph
                .predecessors(battery_id)?
                .filter(|c| c.category() == crate::client::ElectricalComponentCategory::Inverter)
                .map(|c| c.id)
                .collect::<BTreeSet<_>>();

            if group_inverters.is_empty() {
                let e = format!("Battery {} is not connected to any inverters.", battery_id);
                tracing::error!("{}", e);
                return Err(Error::component_data_error(e));
            }

            let mut group_batteries = BTreeSet::new();
            for inverter_id in &group_inverters {
                let connected_batteries = graph
                    .successors(*inverter_id)?
                    .map(|c| c.id)
                    .collect::<BTreeSet<_>>();

                group_batteries.extend(connected_batteries);
            }

            // Ensure that all group batteries are part of the request.
            if !group_batteries.is_subset(&self.component_ids) {
                let e = format!(
                    concat!(
                        "Inverters {:?} are connected to batteries {:?} which are not all in ",
                        "the requested component IDs {:?}"
                    ),
                    group_inverters, group_batteries, self.component_ids
                );

                tracing::error!("{}", e);
                return Err(Error::component_data_error(e));
            }

            // Remove the group batteries from the unvisited set
            unvisited_batteries.retain(|b| !group_batteries.contains(b));

            // Ensure that group batteries are only connect to group inverters
            for battery_id in &group_batteries {
                let connected_inverters = graph
                    .predecessors(*battery_id)?
                    .filter(|c| {
                        c.category() == crate::client::ElectricalComponentCategory::Inverter
                    })
                    .map(|c| c.id)
                    .collect::<BTreeSet<_>>();

                if !connected_inverters.is_subset(&group_inverters) {
                    let e = format!(
                        "Battery {} is connected to inverters {:?} which are not all in the same group {:?}",
                        battery_id, connected_inverters, group_inverters
                    );
                    tracing::error!("{}", e);
                    return Err(Error::component_data_error(e));
                }
            }

            groups.push(InverterBatteryGroup::new(group_inverters, group_batteries));
        }

        Ok(groups)
    }

    pub async fn run(self) -> Result<(), Error> {
        let mut inverter_battery_group_data = HashMap::new();

        let inverter_battery_group_ids = self.get_inverter_battery_groups()?;

        let (component_status_tx, mut component_status_rx) = tokio::sync::mpsc::channel(100);
        for inverter_battery_group in inverter_battery_group_ids {
            let tracker = InverterBatteryGroupTelemetryTracker::new(
                inverter_battery_group,
                self.missing_data_tolerance,
                self.healthy_state_codes.clone(),
                self.client.clone(),
                component_status_tx.clone(),
            );
            // Spawn a task for each group telemetry tracker
            tokio::spawn(tracker.run());
        }

        // Drop the original sender so that the channel will close when all
        // trackers finish.
        drop(component_status_tx);

        let mut interval = tokio::time::interval(Duration::from_millis(200));
        let mut last_sent_status = None;

        loop {
            tokio::select! {
                maybe_status = component_status_rx.recv() => {
                    match maybe_status {
                        Some((group_ids, status)) => {
                            inverter_battery_group_data.insert(group_ids, status);
                        }
                        // Every group tracker has exited and dropped its sender,
                        // so no further updates will arrive. The `interval.tick()`
                        // arm never disables, so the `select!` `else` can never
                        // run; break here instead.
                        None => break,
                    }
                },
                _ = interval.tick() => {
                    if last_sent_status.as_ref() == Some(&inverter_battery_group_data) {
                        continue; // Skip sending if the status hasn't changed
                    }
                    if let Err(e) = self.component_pool_status_tx.send(
                        BatteryPoolSnapshot(inverter_battery_group_data.clone())
                    )
                    {
                        tracing::error!("Failed to send pool snapshot: {}", e);
                        break;
                    }
                    last_sent_status = Some(inverter_battery_group_data.clone());
                },
            }
        }

        let err = format!(
            "BatteryPoolTelemetryTracker (component IDs {:?}) stopped receiving group telemetry updates.",
            self.component_ids
        );

        tracing::error!("{}", err);

        Err(Error::component_data_error(err))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::TimeDelta;

    use super::BatteryPoolSnapshot;
    use crate::{
        LogicalMeterConfig, LogicalMeterHandle, MicrogridClientHandle,
        client::{
            proto::common::microgrid::electrical_components::ElectricalComponentStateCode,
            test_utils::{MockComponent, MockMicrogridApiClient},
        },
        microgrid::{
            battery_pool::BatteryPool,
            telemetry_tracker::{
                battery_pool_telemetry_tracker::InverterBatteryGroup,
                inverter_battery_group_telemetry_tracker::InverterBatteryGroupStatus,
            },
        },
    };

    impl BatteryPoolSnapshot {
        pub(crate) fn from_groups(
            groups: HashMap<InverterBatteryGroup, InverterBatteryGroupStatus>,
        ) -> Self {
            Self(groups)
        }
    }
    async fn new_pool(graph: MockComponent) -> BatteryPool {
        let api = MockMicrogridApiClient::new(graph);
        let client = MicrogridClientHandle::new_from_client(api);
        let lm = LogicalMeterHandle::try_new(
            client.clone(),
            LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap()),
        )
        .await
        .unwrap();
        BatteryPool::try_new(None, client, lm).unwrap()
    }

    /// Drains `rx` for up to `steps` * 100ms of simulated time, returning the
    /// last snapshot seen.
    async fn last_snapshot(
        rx: &mut tokio::sync::broadcast::Receiver<BatteryPoolSnapshot>,
        steps: u32,
    ) -> BatteryPoolSnapshot {
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
    async fn single_group_reaches_healthy_state() {
        // grid → meter → battery_inverter(3) → battery(4)
        let mut pool = new_pool(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                    MockComponent::battery_inverter(3)
                        .with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
                        .with_children(vec![
                            MockComponent::battery(4)
                                .with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                        ]),
                ]),
        ]))
        .await;

        let mut rx = pool.telemetry_snapshots();
        let snap = last_snapshot(&mut rx, 10).await;

        let groups = snap.groups();
        assert_eq!(
            groups.len(),
            1,
            "expected exactly one inverter-battery group"
        );

        let (group, status) = groups.iter().next().unwrap();
        assert_eq!(group.inverter_ids, [3].into());
        assert_eq!(group.battery_ids, [4].into());
        assert!(status.healthy_inverters.contains_key(&3));
        assert!(status.healthy_batteries.contains_key(&4));
        assert!(status.unhealthy_inverters.is_empty());
        assert!(status.unhealthy_batteries.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn two_disjoint_groups_both_appear_in_snapshot() {
        // grid → meter → [battery_inverter(3)→battery(4), battery_inverter(5)→battery(6)]
        let mut pool = new_pool(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                    MockComponent::battery_inverter(3)
                        .with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
                        .with_children(vec![
                            MockComponent::battery(4)
                                .with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                        ]),
                    MockComponent::battery_inverter(5)
                        .with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
                        .with_children(vec![
                            MockComponent::battery(6)
                                .with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                        ]),
                ]),
        ]))
        .await;

        let mut rx = pool.telemetry_snapshots();
        let snap = last_snapshot(&mut rx, 10).await;

        let groups = snap.groups();
        assert_eq!(groups.len(), 2);

        let all_inverters: std::collections::BTreeSet<u64> = groups
            .keys()
            .flat_map(|g| g.inverter_ids.iter().copied())
            .collect();
        let all_batteries: std::collections::BTreeSet<u64> = groups
            .keys()
            .flat_map(|g| g.battery_ids.iter().copied())
            .collect();
        assert_eq!(all_inverters, [3, 5].into());
        assert_eq!(all_batteries, [4, 6].into());

        for status in groups.values() {
            assert!(status.unhealthy_inverters.is_empty());
            assert!(status.unhealthy_batteries.is_empty());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn calling_telemetry_snapshots_twice_reuses_sender() {
        let mut pool = new_pool(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                    MockComponent::battery_inverter(3)
                        .with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
                        .with_children(vec![
                            MockComponent::battery(4)
                                .with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                        ]),
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
    async fn components_become_unhealthy_when_data_stops() {
        // Both components emit only a handful of samples and then go silent;
        // the stream stays open so the client actor doesn't reconnect and
        // resupply data.
        let mut pool = new_pool(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                    MockComponent::battery_inverter(3)
                        .with_power(vec![0.0, 0.0, 0.0])
                        .with_silence_after_metrics()
                        .with_children(vec![
                            MockComponent::battery(4)
                                .with_power(vec![0.0, 0.0, 0.0])
                                .with_silence_after_metrics(),
                        ]),
                ]),
        ]))
        .await;

        let mut rx = pool.telemetry_snapshots();

        // First: drain past the healthy phase and confirm components reach a
        // healthy state (3 samples over ~600ms).
        let healthy = last_snapshot(&mut rx, 10).await;
        let (_, status) = healthy.groups().iter().next().unwrap();
        assert!(
            status.healthy_inverters.contains_key(&3) && status.healthy_batteries.contains_key(&4),
            "expected components to go healthy after initial samples, got {:?}",
            status
        );

        // Now advance well past the 10s missing-data tolerance — the
        // component telemetry trackers should fire their interval and
        // reclassify both components as unhealthy.
        tokio::time::advance(std::time::Duration::from_secs(15)).await;
        let unhealthy = last_snapshot(&mut rx, 5).await;

        let (_, status) = unhealthy.groups().iter().next().unwrap();
        assert!(
            status.healthy_inverters.is_empty(),
            "inverter should be unhealthy after data stops, got healthy set {:?}",
            status.healthy_inverters.keys()
        );
        assert!(
            status.healthy_batteries.is_empty(),
            "battery should be unhealthy after data stops, got healthy set {:?}",
            status.healthy_batteries.keys()
        );
        assert!(status.unhealthy_inverters.contains_key(&3));
        assert!(status.unhealthy_batteries.contains_key(&4));
    }

    #[tokio::test(start_paused = true)]
    async fn component_with_bad_state_is_unhealthy() {
        // Battery reports an Error state — it must land in the unhealthy
        // set even though samples keep arriving.
        let mut pool = new_pool(MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![
                    MockComponent::battery_inverter(3)
                        .with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
                        .with_children(vec![
                            MockComponent::battery(4)
                                .with_power(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
                                .with_state(ElectricalComponentStateCode::Error),
                        ]),
                ]),
        ]))
        .await;

        let mut rx = pool.telemetry_snapshots();
        let snap = last_snapshot(&mut rx, 10).await;

        let (_, status) = snap.groups().iter().next().unwrap();
        assert!(
            status.healthy_inverters.contains_key(&3),
            "inverter with Ready state should be healthy"
        );
        assert!(
            !status.healthy_batteries.contains_key(&4),
            "battery with Error state should not be in healthy set"
        );
        assert!(
            status.unhealthy_batteries.contains_key(&4),
            "battery with Error state should be in unhealthy set, got {:?}",
            status
        );
    }
}
