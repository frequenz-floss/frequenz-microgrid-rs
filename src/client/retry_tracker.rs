// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! A simple retry tracker for managing connection retries to the API service.

use std::time::Duration;

/// How long to wait after a failure before retrying.
const RETRY_DELAY: Duration = Duration::from_secs(3);

pub(super) struct RetryTracker {
    last_failure: tokio::time::Instant,
}

impl RetryTracker {
    pub(super) fn new() -> Self {
        Self {
            last_failure: tokio::time::Instant::now(),
        }
    }

    /// Marks that the last attempt to connect to the API failed.
    ///
    /// The `next_retry_time` method can be used to determine when the next
    /// retry should be attempted.
    ///
    /// If the last attempt was successful, this method should not be called.
    pub(super) fn mark_new_failure(&mut self) {
        self.last_failure = tokio::time::Instant::now();
    }

    /// Returns the next retry time: a fixed delay after the last failure.
    ///
    /// So a stream whose retries ran late isn't overdue again right after it
    /// fails.
    pub(super) fn next_retry_time(&self) -> tokio::time::Instant {
        self.last_failure + RETRY_DELAY
    }
}
