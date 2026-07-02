// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Bounds tracker for a pool of PV inverters.
//!
//! Subscribes to a [`PvPoolSnapshot`] stream and, for each update, extracts the
//! bounds of a target metric from every healthy PV inverter and aggregates them
//! into a single pool-level set of bounds.
//!
//! PV inverters in a pool are wired in parallel, so their bounds are simply
//! added together.

use std::marker::PhantomData;

use tokio::sync::broadcast;

use crate::client::proto::common::metrics::Bounds as PbBounds;
use crate::microgrid::pool_bounds::compute_pv_pool_bounds;
use crate::microgrid::telemetry_tracker::pv_pool_telemetry_tracker::PvPoolSnapshot;
use crate::{Bounds, metric::Metric};

/// Tracks and aggregates power bounds for a PV pool.
///
/// `M` is the metric used to read bounds from the PV inverters (e.g.
/// `AcPowerActive`).
pub(crate) struct PvPoolBoundsTracker<M: Metric> {
    pool_status_rx: broadcast::Receiver<PvPoolSnapshot>,
    pool_bounds_tx: broadcast::Sender<Vec<Bounds<M::QuantityType>>>,
    _marker: PhantomData<M>,
}

impl<M> PvPoolBoundsTracker<M>
where
    M: Metric,
    Bounds<M::QuantityType>: From<PbBounds>,
{
    pub(crate) fn new(
        pool_status_rx: broadcast::Receiver<PvPoolSnapshot>,
        pool_bounds_tx: broadcast::Sender<Vec<Bounds<M::QuantityType>>>,
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
                    let bounds = compute_pv_pool_bounds::<M>(&pool_status);
                    if self.pool_bounds_tx.send(bounds).is_err() {
                        tracing::debug!(
                            "No receivers for {} PV bounds tracker; shutting down.",
                            M::str_name(),
                        );
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(
                        "{} PV bounds tracker lagged by {n} pool status updates.",
                        M::str_name(),
                    );
                }
                Err(broadcast::error::RecvError::Closed) => {
                    // The telemetry tracker upstream has shut down — a normal
                    // teardown of the whole pool, not an error here.
                    tracing::debug!(
                        "Pool status channel closed; {} PV bounds tracker shutting down.",
                        M::str_name(),
                    );
                    break;
                }
            }
        }
    }
}
