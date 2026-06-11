// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Shared helper for reusing a pool's broadcast producer task.
//!
//! Both [`BatteryPool`](super::BatteryPool) and [`PvPool`](super::PvPool) hand
//! out receivers for a long-lived tracker task whose [`broadcast::Sender`] they
//! hold only as a [`broadcast::WeakSender`]. A new subscription should reuse the
//! running task while it still has live receivers, and start a fresh one
//! otherwise.

use tokio::sync::broadcast;

/// Returns a receiver for the broadcast referenced by `weak` if its sender is
/// still alive and has at least one receiver, signalling that the producer task
/// is still running and worth reusing. Returns `None` when the caller must
/// start a new producer.
pub(super) fn try_reuse<T: Clone>(
    weak: &Option<broadcast::WeakSender<T>>,
) -> Option<broadcast::Receiver<T>> {
    weak.as_ref()
        .and_then(broadcast::WeakSender::upgrade)
        .filter(|tx| tx.receiver_count() > 0)
        .map(|tx| tx.subscribe())
}
