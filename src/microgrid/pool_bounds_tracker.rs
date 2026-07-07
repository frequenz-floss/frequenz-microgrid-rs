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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::broadcast;

    use super::PoolBoundsTracker;
    use crate::metric::AcPowerActive;
    use crate::microgrid::caching_sender::CachingSender;
    use crate::microgrid::pool_bounds::compute_pv_pool_bounds as compute_pool_bounds;
    use crate::microgrid::telemetry_tracker::pv_pool_telemetry_tracker::PvPoolSnapshot;

    #[tokio::test]
    async fn exits_when_snapshot_channel_closes() {
        // The tracker exits on either of two conditions: its own bounds
        // consumers all drop (the tick's receiver-count check), or the upstream
        // snapshot stream goes away (the pool torn down). This covers the latter:
        // dropping the snapshot sender must end the task.
        let (snapshot_tx, snapshot_rx) = broadcast::channel::<PvPoolSnapshot>(16);
        let bounds_tx = CachingSender::new();
        // Hold a bounds receiver so the tracker doesn't stop for want of
        // consumers.
        let _bounds_rx = bounds_tx.subscribe_with_current();
        let handle = tokio::spawn(
            PoolBoundsTracker::new(
                snapshot_rx,
                bounds_tx,
                compute_pool_bounds::<AcPowerActive>,
                "test".to_string(),
            )
            .run(),
        );

        // Let the tracker park on `recv`, then close the snapshot channel.
        tokio::task::yield_now().await;
        drop(snapshot_tx);
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("bounds tracker should exit when the snapshot channel closes")
            .expect("bounds tracker task panicked");
    }

    #[tokio::test(start_paused = true)]
    async fn exits_when_all_bounds_consumers_drop() {
        // The other exit condition: the snapshot stream stays open (the pool is
        // alive) but the tracker's own bounds consumers all drop. A stable
        // snapshot never wakes the `recv` arm, so the periodic tick's
        // receiver-count check is what must end the task.
        let (_snapshot_tx, snapshot_rx) = broadcast::channel::<PvPoolSnapshot>(16);
        let bounds_tx = CachingSender::new();
        let bounds_rx = bounds_tx.subscribe_with_current();
        let handle = tokio::spawn(
            PoolBoundsTracker::new(
                snapshot_rx,
                bounds_tx,
                compute_pool_bounds::<AcPowerActive>,
                "test".to_string(),
            )
            .run(),
        );

        // Let the tracker park, then drop its only consumer. `_snapshot_tx` stays
        // alive, so the exit is attributable to the dropped consumer rather than a
        // closed snapshot channel.
        tokio::task::yield_now().await;
        drop(bounds_rx);
        // The next tick must observe zero receivers and shut the tracker down.
        tokio::time::advance(Duration::from_millis(200)).await;
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("bounds tracker should exit once its consumers drop")
            .expect("bounds tracker task panicked");
    }
}
