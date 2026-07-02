// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Bounds tracker for pools of microgrid components.
//!
//! Subscribes to a [`BatteryPoolSnapshot`] stream and, for each update, extracts
//! the bounds of a target metric from every healthy component and aggregates
//! them into a single pool-level set of bounds.
//!
//! Aggregation follows the physical topology of an inverter-battery group in
//! an `MxN` configuration (M inverters wired in parallel to N batteries wired
//! in parallel, with the inverter side in series with the battery side):
//!
//! * Healthy inverters within a group are in parallel — their bounds are
//!   added together.
//! * Healthy batteries within a group are in parallel — their bounds are
//!   added together.
//! * The inverter side and battery side of a group are in series — their
//!   aggregated bounds are intersected.
//! * Groups within a pool are in parallel — their bounds are added together.

use std::marker::PhantomData;

use tokio::sync::broadcast;

use crate::client::proto::common::metrics::Bounds as PbBounds;
use crate::microgrid::pool_bounds::compute_battery_pool_bounds;
use crate::microgrid::telemetry_tracker::battery_pool_telemetry_tracker::BatteryPoolSnapshot;
use crate::{Bounds, metric::Metric};

/// Tracks and aggregates power bounds for a battery pool.
///
/// `InverterM` is the metric used to read bounds from inverters (e.g.
/// `AcPowerActive`); `BatteryM` is the metric used to read bounds from
/// batteries (e.g. `DcPower`). Both must share the same `QuantityType` so
/// their bounds can be intersected and summed.
pub(crate) struct BatteryPoolBoundsTracker<InverterM: Metric, BatteryM: Metric> {
    pool_status_rx: broadcast::Receiver<BatteryPoolSnapshot>,
    pool_bounds_tx: broadcast::Sender<Vec<Bounds<InverterM::QuantityType>>>,
    _marker: PhantomData<(InverterM, BatteryM)>,
}

impl<InverterM, BatteryM> BatteryPoolBoundsTracker<InverterM, BatteryM>
where
    InverterM: Metric,
    BatteryM: Metric<QuantityType = InverterM::QuantityType>,
    Bounds<InverterM::QuantityType>: From<PbBounds>,
{
    pub(crate) fn new(
        pool_status_rx: broadcast::Receiver<BatteryPoolSnapshot>,
        pool_bounds_tx: broadcast::Sender<Vec<Bounds<InverterM::QuantityType>>>,
    ) -> Self {
        Self {
            pool_status_rx,
            pool_bounds_tx,
            _marker: PhantomData,
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            match self.pool_status_rx.recv().await {
                Ok(pool_status) => {
                    let bounds = compute_battery_pool_bounds::<InverterM, BatteryM>(&pool_status);
                    if self.pool_bounds_tx.send(bounds).is_err() {
                        tracing::debug!(
                            "No receivers for {}/{} bounds tracker; shutting down.",
                            InverterM::str_name(),
                            BatteryM::str_name(),
                        );
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(
                        "{}/{} bounds tracker lagged by {n} pool status updates.",
                        InverterM::str_name(),
                        BatteryM::str_name(),
                    );
                }
                Err(broadcast::error::RecvError::Closed) => {
                    // The telemetry tracker upstream has shut down — a normal
                    // teardown of the whole pool, not an error here.
                    tracing::debug!(
                        "Pool status channel closed; {}/{} bounds tracker shutting down.",
                        InverterM::str_name(),
                        BatteryM::str_name(),
                    );
                    break;
                }
            }
        }
    }
}
