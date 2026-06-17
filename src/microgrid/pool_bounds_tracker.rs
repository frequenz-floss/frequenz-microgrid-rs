// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! A generic bounds tracker for a pool of microgrid components.
//!
//! Subscribes to a pool snapshot stream and, for each update, computes a
//! pool-level set of bounds with a caller-supplied function and broadcasts it.
//! The aggregation logic (which differs per pool type — see
//! [`pv_bounds_tracker`](super::pv_bounds_tracker) and
//! [`battery_bounds_tracker`](super::battery_bounds_tracker)) is injected as a
//! plain function, so this loop is shared across pool types.

use tokio::sync::broadcast;

use crate::{Bounds, quantity::Quantity};

/// Tracks and aggregates power bounds for a pool.
///
/// `S` is the pool snapshot type and `Q` the quantity the bounds are expressed
/// in. `compute` maps a snapshot to the aggregated pool bounds; `label`
/// identifies the tracker in log messages.
pub(crate) struct PoolBoundsTracker<S, Q: Quantity> {
    pool_status_rx: broadcast::Receiver<S>,
    pool_bounds_tx: broadcast::Sender<Vec<Bounds<Q>>>,
    compute: fn(&S) -> Vec<Bounds<Q>>,
    label: String,
}

impl<S, Q> PoolBoundsTracker<S, Q>
where
    S: Clone,
    Q: Quantity,
{
    pub(crate) fn new(
        pool_status_rx: broadcast::Receiver<S>,
        pool_bounds_tx: broadcast::Sender<Vec<Bounds<Q>>>,
        compute: fn(&S) -> Vec<Bounds<Q>>,
        label: String,
    ) -> Self {
        Self {
            pool_status_rx,
            pool_bounds_tx,
            compute,
            label,
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            match self.pool_status_rx.recv().await {
                Ok(pool_status) => {
                    let bounds = (self.compute)(&pool_status);
                    if self.pool_bounds_tx.send(bounds).is_err() {
                        tracing::debug!(
                            "No receivers for {} bounds tracker; shutting down.",
                            self.label,
                        );
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(
                        "{} bounds tracker lagged by {n} pool status updates.",
                        self.label,
                    );
                }
                Err(broadcast::error::RecvError::Closed) => {
                    // The telemetry tracker upstream has shut down — a normal
                    // teardown of the whole pool, not an error here.
                    tracing::debug!(
                        "Pool status channel closed; {} bounds tracker shutting down.",
                        self.label,
                    );
                    break;
                }
            }
        }
    }
}
