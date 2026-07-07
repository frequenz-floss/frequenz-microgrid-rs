// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! A telemetry tracker for a pool of batteries and their associated inverters.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    time::Duration,
};

use frequenz_microgrid_component_graph::ComponentGraph;

use crate::{
    Error, LogicalMeterHandle, MicrogridClientHandle,
    client::proto::common::microgrid::electrical_components::{
        ElectricalComponent, ElectricalComponentConnection, ElectricalComponentStateCode,
    },
    microgrid::caching_sender::CachingSender,
    microgrid::telemetry_tracker::component_partition::ComponentHealthPartition,
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

#[derive(Clone, Debug, Default, PartialEq)]
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
pub(crate) struct BatteryPoolTelemetryTracker {
    component_ids: BTreeSet<u64>,
    component_pool_status_tx: CachingSender<BatteryPoolSnapshot>,
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
        component_pool_status_tx: CachingSender<BatteryPoolSnapshot>,
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

    /// Walks the component graph to partition `component_ids` (battery IDs) into
    /// inverter-battery groups, validating that the selection is complete: each
    /// battery must reach only inverters whose other batteries are also in the
    /// set. Returns an [`Error`] for a malformed or partial selection.
    ///
    /// An empty `component_ids` set is a valid (empty) pool: the loop visits no
    /// batteries and yields no groups.
    pub(crate) fn inverter_battery_groups(
        graph: &ComponentGraph<ElectricalComponent, ElectricalComponentConnection>,
        component_ids: &BTreeSet<u64>,
    ) -> Result<Vec<InverterBatteryGroup>, Error> {
        let mut unvisited_batteries = component_ids.clone();
        let mut groups = Vec::new();

        while let Some(battery_id) = unvisited_batteries.iter().next().cloned() {
            let group_inverters = graph
                .predecessors(battery_id)
                .map_err(|e| {
                    tracing::error!("Failed to query predecessors of battery {battery_id}: {e}");
                    e
                })?
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
                    .successors(*inverter_id)
                    .map_err(|e| {
                        tracing::error!(
                            "Failed to query successors of inverter {inverter_id}: {e}"
                        );
                        e
                    })?
                    .map(|c| c.id)
                    .collect::<BTreeSet<_>>();

                group_batteries.extend(connected_batteries);
            }

            // Ensure that all group batteries are part of the request.
            if !group_batteries.is_subset(component_ids) {
                let e = format!(
                    concat!(
                        "Inverters {:?} are connected to batteries {:?} which are not all in ",
                        "the requested component IDs {:?}"
                    ),
                    group_inverters, group_batteries, component_ids
                );

                tracing::error!("{}", e);
                return Err(Error::component_data_error(e));
            }

            // Remove the group batteries from the unvisited set
            unvisited_batteries.retain(|b| !group_batteries.contains(b));

            // Ensure that group batteries are only connect to group inverters
            for battery_id in &group_batteries {
                let connected_inverters = graph
                    .predecessors(*battery_id)
                    .map_err(|e| {
                        tracing::error!(
                            "Failed to query predecessors of battery {battery_id}: {e}"
                        );
                        e
                    })?
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

    pub(crate) async fn run(self) {
        // Errors are logged at source inside `inverter_battery_groups`.
        let Ok(inverter_battery_group_ids) =
            Self::inverter_battery_groups(self.logical_meter.graph(), &self.component_ids)
        else {
            // Construction (`BatteryPool::try_new`) already validated the
            // topology, so this only fires on a transient graph-query failure.
            // Return without publishing: dropping the sender closes the stream,
            // which a subscriber can tell apart from a valid empty snapshot,
            // instead of passing off a malformed pool as an empty one.
            return;
        };

        let is_empty_pool = inverter_battery_group_ids.is_empty();

        // Seed each group as all-unhealthy so the initial snapshot reflects the
        // pool's real membership (every group present, unhealthy until its data
        // arrives) rather than an empty map.
        let mut snapshot = BatteryPoolSnapshot(
            inverter_battery_group_ids
                .iter()
                .map(|group| {
                    let mut inverters = ComponentHealthPartition::default();
                    for &inverter_id in &group.inverter_ids {
                        inverters.mark_unhealthy(inverter_id, None);
                    }
                    let mut batteries = ComponentHealthPartition::default();
                    for &battery_id in &group.battery_ids {
                        batteries.mark_unhealthy(battery_id, None);
                    }
                    (
                        group.clone(),
                        InverterBatteryGroupStatus {
                            inverters,
                            batteries,
                        },
                    )
                })
                .collect(),
        );

        // Publish the initial (seeded, all-unhealthy) snapshot before opening the
        // group streams, so a fresh subscriber gets it (or its cached copy) at
        // once. Ignore "no receivers" here — the tick loop below owns shutdown.
        let _ = self.component_pool_status_tx.publish(snapshot.clone());

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

        // Drop the original sender so the channel closes once every group tracker
        // finishes, ending the loop below. An empty pool spawns no trackers, so
        // keep the sender instead: `recv()` then parks, and the tick loop drives
        // the (empty) snapshot and the receiver-count shutdown check — so the
        // task stops when its consumers go, not before.
        let _empty_pool_keepalive = if is_empty_pool {
            Some(component_status_tx)
        } else {
            drop(component_status_tx);
            None
        };

        let mut interval = tokio::time::interval(Duration::from_millis(200));

        loop {
            tokio::select! {
                maybe_status = component_status_rx.recv() => {
                    match maybe_status {
                        Some((group_ids, status)) => {
                            snapshot.0.insert(group_ids, status);
                        }
                        // Every group tracker has exited and dropped its sender,
                        // so no further updates will ever arrive. The `_ =
                        // interval.tick()` arm below is a catch-all that never
                        // disables, so the `select!` `else` branch can never run;
                        // break here instead.
                        None => break,
                    }
                },
                _ = interval.tick() => {
                    // Publish only when the groups changed; either way, stop once
                    // the last consumer has dropped.
                    if !self.component_pool_status_tx.publish_if_changed(&snapshot) {
                        break;
                    }
                },
            }
        }

        // Reaching here means either every consumer dropped or every group
        // tracker exited — a normal shutdown, not an error.
        tracing::debug!(
            "BatteryPoolTelemetryTracker (component IDs {:?}) stopped: all consumers or group trackers are gone.",
            self.component_ids
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::BatteryPoolSnapshot;
    use crate::client::proto::common::microgrid::electrical_components::ElectricalComponentStateCode;
    use crate::client::test_utils::MockComponent;
    use crate::microgrid::battery_pool::BatteryPool;
    use crate::microgrid::telemetry_tracker::battery_pool_telemetry_tracker::InverterBatteryGroup;
    use crate::microgrid::telemetry_tracker::inverter_battery_group_telemetry_tracker::InverterBatteryGroupStatus;
    use crate::microgrid::test_utils::{handles, last_snapshot};

    impl BatteryPoolSnapshot {
        pub(crate) fn from_groups(
            groups: HashMap<InverterBatteryGroup, InverterBatteryGroupStatus>,
        ) -> Self {
            Self(groups)
        }
    }
    async fn new_pool(graph: MockComponent) -> BatteryPool {
        let (client, lm) = handles(graph).await;
        BatteryPool::try_new(None, client, lm).unwrap()
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
        assert!(status.inverters.healthy.contains_key(&3));
        assert!(status.batteries.healthy.contains_key(&4));
        assert!(status.inverters.unhealthy.is_empty());
        assert!(status.batteries.unhealthy.is_empty());
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
            assert!(status.inverters.unhealthy.is_empty());
            assert!(status.batteries.unhealthy.is_empty());
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
            status.inverters.healthy.contains_key(&3) && status.batteries.healthy.contains_key(&4),
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
            status.inverters.healthy.is_empty(),
            "inverter should be unhealthy after data stops, got healthy set {:?}",
            status.inverters.healthy.keys()
        );
        assert!(
            status.batteries.healthy.is_empty(),
            "battery should be unhealthy after data stops, got healthy set {:?}",
            status.batteries.healthy.keys()
        );
        assert!(status.inverters.unhealthy.contains_key(&3));
        assert!(status.batteries.unhealthy.contains_key(&4));
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
            status.inverters.healthy.contains_key(&3),
            "inverter with Ready state should be healthy"
        );
        assert!(
            !status.batteries.healthy.contains_key(&4),
            "battery with Error state should not be in healthy set"
        );
        assert!(
            status.batteries.unhealthy.contains_key(&4),
            "battery with Error state should be in unhealthy set, got {:?}",
            status
        );
    }
}
