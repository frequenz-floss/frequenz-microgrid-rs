// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! A simple retry tracker for managing connection retries to the API service.

use std::time::Duration;

pub(super) struct RetryTracker {
    failure_start: tokio::time::Instant,
    num_failures: u64,
    new_failure: bool,
}

impl RetryTracker {
    pub(super) fn new() -> Self {
        Self {
            failure_start: tokio::time::Instant::now(),
            num_failures: 0,
            new_failure: true,
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
        self.new_failure = true;
    }

    /// Returns the next retry time based on the number of failures.
    ///
    /// If the previous retry attempt was not marked as a failure, this method
    /// returns `None`, indicating that no retry is needed at this time.
    pub(super) fn next_retry_time(&self) -> Option<tokio::time::Instant> {
        if !self.new_failure {
            return None;
        }
        Some(self.failure_start + Duration::from_secs(self.num_failures * 3))
    }

    /// Marks that a retry attempt is being made now, resetting the failure
    /// state.
    ///
    /// This approach ensures that a new retry attempt doesn't get made before
    /// the previous attempt has finished and been marked as a failure.
    pub(super) fn mark_new_retry(&mut self) {
        self.new_failure = false;
    }
}
