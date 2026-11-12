// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! A simple retry tracker for managing connection retries to the API service.

use std::time::Duration;

pub(super) struct RetryTracker {
    failure_start: tokio::time::Instant,
    num_failures: u64,
}

impl RetryTracker {
    pub(super) fn new() -> Self {
        Self {
            failure_start: tokio::time::Instant::now(),
            num_failures: 0,
        }
    }

    /// Marks that the last attempt to connect to the API failed.
    ///
    /// The `next_retry_time` method can be used to determine when the next
    /// retry should be attempted.
    ///
    /// If the last attempt was successful, this method should not be called.
    pub(super) fn mark_new_failure(&mut self) {
        self.num_failures += 1;
    }

    /// Returns the next retry time based on the number of failures.
    pub(super) fn next_retry_time(&self) -> tokio::time::Instant {
        self.failure_start + Duration::from_secs(self.num_failures * 3)
    }
}
