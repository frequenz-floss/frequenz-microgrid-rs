// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Telemetry trackers for microgrid component pools.
//!
//! Each tracker watches a set of components and emits a stream of snapshots
//! that partition the components into healthy and unhealthy sets, carrying
//! the latest telemetry sample for each.

pub(crate) mod battery_pool_telemetry_tracker;
pub(crate) mod component_partition;
pub(crate) mod component_telemetry_tracker;
pub(crate) mod inverter_battery_group_telemetry_tracker;
pub(crate) mod pv_pool_telemetry_tracker;
