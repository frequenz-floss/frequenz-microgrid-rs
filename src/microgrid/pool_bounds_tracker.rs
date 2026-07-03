// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! A generic bounds tracker for a pool of microgrid components.
//!
//! Subscribes to a pool snapshot stream and, for each update, computes a
//! pool-level set of bounds with a caller-supplied function and publishes it.
//! The aggregation logic (which differs per pool type — see
//! [`pool_bounds`](super::pool_bounds)) is injected as a
//! plain function, so this loop is shared across pool types.

use std::time::Duration;

use tokio::sync::broadcast;

use crate::microgrid::caching_sender::CachingSender;
use crate::{Bounds, quantity::Quantity};

/// Tracks and aggregates power bounds for a pool.
///
/// `S` is the pool snapshot type and `Q` the quantity the bounds are expressed
/// in. `compute` maps a snapshot to the aggregated pool bounds; `label`
/// identifies the tracker in log messages.
pub(crate) struct PoolBoundsTracker<S, Q: Quantity, F> {
    pool_status_rx: broadcast::Receiver<S>,
    pool_bounds_tx: CachingSender<Vec<Bounds<Q>>>,
    compute: F,
    label: String,
}

impl<S, Q, F> PoolBoundsTracker<S, Q, F>
where
    S: Clone,
    Q: Quantity,
    F: Fn(&S) -> Vec<Bounds<Q>>,
{
    pub(crate) fn new(
        pool_status_rx: broadcast::Receiver<S>,
        pool_bounds_tx: CachingSender<Vec<Bounds<Q>>>,
        compute: F,
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
        let mut interval = tokio::time::interval(Duration::from_millis(200));
        // The loop breaks with the reason it stops, logged once below.
        let reason = loop {
            tokio::select! {
                recv = self.pool_status_rx.recv() => {
                    match recv {
                        Ok(snapshot) => {
                            // Recompute from the new snapshot and publish only on
                            // change; stop once the last bounds receiver dropped.
                            let bounds = (self.compute)(&snapshot);
                            if !self.pool_bounds_tx.publish_if_changed(&bounds) {
                                break "no receivers";
                            }
                        }
                        // A slow consumer made us miss snapshots; recompute from
                        // the next one that arrives.
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(
                                "{} bounds tracker lagged by {n} pool status updates.",
                                self.label,
                            );
                        }
                        // The snapshot stream has been torn down (the whole pool
                        // is gone) — a normal shutdown.
                        Err(broadcast::error::RecvError::Closed) => {
                            break "pool status channel closed";
                        }
                    }
                }
                _ = interval.tick() => {
                    // A stable snapshot never wakes the `recv` arm, so check here
                    // that our own consumers are still present.
                    if self.pool_bounds_tx.receiver_count() == 0 {
                        break "no receivers";
                    }
                }
            }
        };
        tracing::debug!("{} bounds tracker shutting down: {reason}.", self.label);
    }
}
