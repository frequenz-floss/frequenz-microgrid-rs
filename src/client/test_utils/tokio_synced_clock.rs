// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Test clock for `#[tokio::test(start_paused = true)]` tests.
//!
//! Pins wall-clock time to tokio's (possibly paused) monotonic clock, so
//! advancing tokio time advances wall time too. Clones share state, so a
//! test can hand copies to the actor and a mock telemetry source and jump
//! both by calling
//! [`inject_wall_jump`][TokioSyncedClock::inject_wall_jump] once.
//!
//! Lives under `test_utils` so the `Arc`/`RwLock`/injection machinery
//! isn't compiled into production builds.

use chrono::{DateTime, TimeDelta, Utc};
use tokio::time::Instant;

use crate::wall_clock_timer::Clock;

#[derive(Clone, Debug)]
pub struct TokioSyncedClock {
    inner: std::sync::Arc<std::sync::RwLock<TokioSyncedClockInner>>,
}

#[derive(Debug)]
struct TokioSyncedClockInner {
    wall_anchor: DateTime<Utc>,
    mono_anchor: Instant,
}

impl Default for TokioSyncedClock {
    fn default() -> Self {
        Self::new()
    }
}

impl TokioSyncedClock {
    /// Creates a clock anchored to the current `Utc::now()` (and the current
    /// tokio monotonic instant). Suitable when the caller doesn't care about
    /// a specific starting wall-clock value.
    pub fn new() -> Self {
        Self::with_wall_anchor(Utc::now())
    }

    /// Creates a clock whose wall-clock time at the current tokio instant
    /// is exactly `wall_anchor`. Useful when the caller needs the anchor
    /// aligned to a specific boundary (e.g. a whole-second tick).
    pub fn with_wall_anchor(wall_anchor: DateTime<Utc>) -> Self {
        Self {
            inner: std::sync::Arc::new(std::sync::RwLock::new(TokioSyncedClockInner {
                wall_anchor,
                mono_anchor: Instant::now(),
            })),
        }
    }

    /// Shifts wall-clock time by `offset` relative to the monotonic clock,
    /// simulating an NTP jump. Visible to every clone.
    pub fn inject_wall_jump(&self, offset: TimeDelta) {
        let mut inner = self.inner.write().expect("clock poisoned");
        // Only `wall_anchor` moves — `mono_anchor` is intentionally left
        // untouched so wall and monotonic diverge, which is what makes
        // this simulate an NTP jump rather than a re-anchor of both
        // clocks together.
        inner.wall_anchor += offset;
    }
}

impl Clock for TokioSyncedClock {
    fn wall_now(&self) -> DateTime<Utc> {
        let inner = self.inner.read().expect("clock poisoned");
        let elapsed = Instant::now().duration_since(inner.mono_anchor);
        inner.wall_anchor
            + TimeDelta::from_std(elapsed).expect("tokio elapsed fits in TimeDelta (~292 years)")
    }
}
