// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Caching broadcast sender for a pool's shared stream.
//!
//! `broadcast` only delivers values sent *after* a receiver subscribes, so a
//! pool's producer publishes through a [`CachingSender`] that remembers the last
//! value it sent; [`subscribe_with_current`](CachingSender::subscribe_with_current)
//! re-sends it to the fresh receiver, so a new or resubscribing consumer observes
//! the current value immediately instead of blocking on a quiet stream. (The
//! re-send also reaches existing receivers as a duplicate of the current value —
//! harmless for the pools' state streams, which only care about the latest
//! value.)

use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

/// Capacity of each pool's broadcast channel.
const CHANNEL_CAPACITY: usize = 100;

/// A `broadcast::Sender` that remembers the last value it sent.
///
/// Producers publish through it (which caches the value), and a fresh subscriber
/// is handed that cached value immediately — working around `broadcast` only
/// delivering values sent after a receiver subscribes.
pub(crate) struct CachingSender<T> {
    tx: broadcast::Sender<T>,
    last: Arc<Mutex<Option<T>>>,
}

impl<T> Clone for CachingSender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            last: Arc::clone(&self.last),
        }
    }
}

impl<T: Clone> CachingSender<T> {
    pub(crate) fn new() -> Self {
        let (tx, _rx) = broadcast::channel(CHANNEL_CAPACITY);
        Self {
            tx,
            last: Arc::new(Mutex::new(None)),
        }
    }

    /// Publishes `value` to all receivers and remembers it as the current value.
    /// Returns `false` when no receiver is listening (the producer's cue to
    /// stop).
    pub(crate) fn publish(&self, value: T) -> bool {
        // Hold the lock across the send so a concurrent `subscribe_with_current`
        // re-send can't reorder ahead of this update and strand a receiver on a
        // stale value.
        let mut last = self.last.lock().unwrap_or_else(|e| e.into_inner());
        *last = Some(value.clone());
        self.tx.send(value).is_ok()
    }

    /// Publishes `value` only when it differs from the last value published,
    /// remembering it either way. Takes `value` by reference and clones only
    /// when it actually sends, so an unchanged value costs no allocation.
    /// Returns `false` when no receiver is listening (whether or not `value`
    /// was sent) — the producer's cue to stop.
    pub(crate) fn publish_if_changed(&self, value: &T) -> bool
    where
        T: PartialEq,
    {
        let mut last = self.last.lock().unwrap_or_else(|e| e.into_inner());
        if last.as_ref() == Some(value) {
            return self.tx.receiver_count() > 0;
        }
        *last = Some(value.clone());
        // Send while holding the lock, as `publish` does, so the update stays
        // ordered against a concurrent re-send.
        self.tx.send(value.clone()).is_ok()
    }

    /// The number of live consumer receivers (senders — held by the producer
    /// and the pool — don't count).
    pub(crate) fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }

    /// Subscribes and immediately re-sends the current value (if any) so the new
    /// receiver isn't left waiting on a quiet stream.
    pub(crate) fn subscribe_with_current(&self) -> broadcast::Receiver<T> {
        // Take the lock before subscribing and re-sending so the re-send is
        // ordered against any concurrent producer publish (which also sends
        // under the lock); otherwise the new receiver could end on a stale value.
        let last = self.last.lock().unwrap_or_else(|e| e.into_inner());
        let rx = self.tx.subscribe();
        if let Some(value) = last.clone() {
            let _ = self.tx.send(value);
        }
        rx
    }

    /// Returns a weak handle that shares this sender's cache but doesn't keep the
    /// channel open. A pool holds one to reuse a running tracker only while the
    /// tracker's own strong sender is still alive.
    pub(crate) fn downgrade(&self) -> WeakCachingSender<T> {
        WeakCachingSender {
            tx: self.tx.downgrade(),
            last: Arc::clone(&self.last),
        }
    }
}

/// A weak handle to a [`CachingSender`], as [`broadcast::WeakSender`] is to
/// [`broadcast::Sender`].
///
/// It doesn't keep the channel open: [`upgrade`](WeakCachingSender::upgrade)
/// yields a live [`CachingSender`] only while at least one strong sender (the
/// producer task's) still exists, so a live receiver alone can't resurrect a
/// producer that has already exited.
pub(crate) struct WeakCachingSender<T> {
    tx: broadcast::WeakSender<T>,
    last: Arc<Mutex<Option<T>>>,
}

impl<T> WeakCachingSender<T> {
    /// Upgrades to a strong [`CachingSender`] sharing the same cache, or `None`
    /// if every strong sender (i.e. the producer task) is already gone.
    pub(crate) fn upgrade(&self) -> Option<CachingSender<T>> {
        self.tx.upgrade().map(|tx| CachingSender {
            tx,
            last: Arc::clone(&self.last),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::CachingSender;

    #[test]
    fn late_subscriber_is_re_sent_the_cached_value() {
        let tx = CachingSender::new();
        let _first = tx.subscribe_with_current();
        assert!(tx.publish(7));

        // A subscriber joining after the publish is re-sent the cached value
        // at once, rather than waiting for the next publish.
        let mut late = tx.subscribe_with_current();
        assert_eq!(
            late.try_recv().expect("cached value re-sent on subscribe"),
            7
        );
    }

    #[test]
    fn publish_reports_whether_anyone_is_listening() {
        let tx = CachingSender::new();
        assert!(!tx.publish(1), "no receivers yet");
        let _rx = tx.subscribe_with_current();
        assert!(tx.publish(2), "a receiver is listening");
    }

    #[test]
    fn publish_if_changed_does_not_resend_an_unchanged_value() {
        let tx = CachingSender::new();
        let mut rx = tx.subscribe_with_current();
        assert!(tx.publish_if_changed(&5));
        assert!(tx.publish_if_changed(&5), "still has a receiver");
        assert!(tx.publish_if_changed(&6));

        assert_eq!(rx.try_recv().unwrap(), 5);
        assert_eq!(rx.try_recv().unwrap(), 6);
        assert!(rx.try_recv().is_err(), "the unchanged 5 was not re-sent");
    }

    #[test]
    fn weak_handle_upgrades_and_shares_the_cache_while_a_producer_lives() {
        let tx = CachingSender::new();
        let _rx = tx.subscribe_with_current();
        assert!(tx.publish(11));
        let weak = tx.downgrade();
        // Stands in for the spawned tracker's strong sender; keep it alive.
        let _producer = tx.clone();
        drop(tx);

        let upgraded = weak
            .upgrade()
            .expect("upgradable while a strong sender lives");
        // The upgraded handle shares the same cache: a fresh subscriber sees 11.
        let mut late = upgraded.subscribe_with_current();
        assert_eq!(late.try_recv().expect("cached value re-sent"), 11);
    }

    #[test]
    fn weak_handle_does_not_upgrade_after_the_producer_exits() {
        let tx = CachingSender::<i32>::new();
        let weak = tx.downgrade();
        // A consumer receiver stays alive...
        let _rx = tx.subscribe_with_current();
        // ...but the producer (the sole strong sender) exits.
        drop(tx);

        // A live receiver no longer implies a live producer, so the pool's weak
        // handle refuses to upgrade instead of reusing a dead channel.
        assert!(weak.upgrade().is_none());
    }
}
