// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! A minimal wall-clock-aligned timer that survives NTP jumps.
//!
//! Sleeps run on tokio's monotonic clock; tick times are expressed on the
//! wall clock. If the two diverge by more than one interval (detected at
//! tick entry), the timer realigns to the wall clock and the next tick
//! fires immediately with `TickInfo::resynced = true`.
//!
//! Scope: this is a deliberately minimal timer for the
//! `LogicalMeterActor` resampling loop. Compared to richer
//! implementations elsewhere, it is always epoch-aligned (no
//! caller-supplied alignment anchor), it does not track gradual
//! wall-clock drift, and it does not warn on async scheduling
//! lateness. Add those if a second caller needs them.

use chrono::{DateTime, TimeDelta, Utc};

/// Abstracts wall-clock time so tests can wire it to tokio's paused clock.
/// Production uses [`SystemClock`]; test-only implementations live in
/// `crate::client::test_utils`.
pub trait Clock: Send + Sync {
    fn wall_now(&self) -> DateTime<Utc>;
}

/// The real system wall clock. `wall_now()` returns `chrono::Utc::now()`.
///
/// Not suitable for `#[tokio::test(start_paused = true)]` tests:
/// `Utc::now()` keeps advancing in real time while tokio's monotonic
/// clock is frozen, so the timer's drift detector immediately sees a
/// huge wall-vs-monotonic skew and resyncs every tick. Use
/// [`crate::client::test_utils::TokioSyncedClock`] for paused-time
/// tests instead.
pub struct SystemClock;

impl Clock for SystemClock {
    fn wall_now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Rounds `timestamp` down to the previous multiple of `interval` since the
/// Unix epoch. Returns `timestamp` unchanged if `interval` is zero,
/// negative, or either value cannot be represented in nanoseconds.
fn align_to_epoch(timestamp: DateTime<Utc>, interval: TimeDelta) -> DateTime<Utc> {
    let Some(interval_nanos) = interval.num_nanoseconds() else {
        return timestamp;
    };
    if interval_nanos <= 0 {
        return timestamp;
    }
    let Some(ts_nanos) = timestamp
        .signed_duration_since(DateTime::<Utc>::UNIX_EPOCH)
        .num_nanoseconds()
    else {
        return timestamp;
    };
    let aligned = ts_nanos.div_euclid(interval_nanos) * interval_nanos;
    DateTime::<Utc>::UNIX_EPOCH + TimeDelta::nanoseconds(aligned)
}

/// Information about a single timer tick.
#[derive(Debug, Clone)]
pub struct TickInfo {
    /// The wall-clock time this tick was expected to fire at. On a resync
    /// tick this is the pre-jump expected time.
    pub expected_tick_time: DateTime<Utc>,
    /// `true` if a wall-clock jump was detected and the timer realigned.
    /// Callers holding timestamp-sensitive state should rebuild it against
    /// the timer's now-realigned `next_tick_time()`.
    pub resynced: bool,
}

/// A wall-clock-aligned periodic timer.
pub struct WallClockTimer<C: Clock> {
    interval: TimeDelta,
    next_tick: DateTime<Utc>,
    clock: C,
    last_wall: DateTime<Utc>,
    last_monotonic: tokio::time::Instant,
}

impl<C: Clock> WallClockTimer<C> {
    /// Returns `Err` if `interval` is non-positive or cannot be represented
    /// as a [`std::time::Duration`]. Validating both at construction lets
    /// `tick()` convert sleep durations infallibly — without this guarantee a
    /// conversion failure would have to silently fall back to a zero
    /// duration, busy-looping the tick loop.
    pub fn try_new(interval: TimeDelta, clock: C) -> Result<Self, crate::Error> {
        if interval <= TimeDelta::zero() {
            return Err(crate::Error::invalid_config(format!(
                "interval must be positive, got {interval:?}",
            )));
        }
        if interval.to_std().is_err() {
            return Err(crate::Error::invalid_config(format!(
                "interval too large for std::time::Duration: {interval:?}",
            )));
        }
        let now = clock.wall_now();
        let next_tick = align_to_epoch(now, interval) + interval;
        Ok(Self {
            interval,
            next_tick,
            clock,
            last_wall: now,
            last_monotonic: tokio::time::Instant::now(),
        })
    }

    /// The wall-clock time the next tick is scheduled for.
    ///
    /// Note: this can move *backwards* across calls if a backward
    /// wall-clock jump triggers a resync; successive return values are
    /// not monotonic. Don't use them for ordering or latency math.
    pub fn next_tick_time(&self) -> DateTime<Utc> {
        self.next_tick
    }

    /// Waits until the next tick, resyncing on a detected wall-clock jump.
    pub async fn tick(&mut self) -> TickInfo {
        // Detect wall-clock jumps by comparing wall-clock elapsed against
        // monotonic elapsed since the last observation. A real NTP jump
        // shifts the wall clock without touching the monotonic clock, so
        // the two diverge. A slow caller (event-loop lateness, long work
        // between ticks) advances both clocks together, so they don't
        // diverge — this avoids spurious resyncs that would otherwise
        // rebuild resamplers and surface a phantom `None` sample whenever
        // the caller takes longer than one interval to re-enter `tick()`.
        //
        // Strict `>`: drift of exactly 1× interval is treated as scheduling
        // jitter, not a jump. Only strictly-greater drifts resync.
        let threshold = self.interval;
        loop {
            let wall_now = self.clock.wall_now();
            let monotonic_now = tokio::time::Instant::now();
            let wall_elapsed = wall_now - self.last_wall;
            let monotonic_elapsed = TimeDelta::from_std(
                monotonic_now.duration_since(self.last_monotonic),
            )
            .unwrap_or_else(|_| {
                tracing::warn!(
                    "monotonic elapsed exceeds TimeDelta range (~292 years); clamping to TimeDelta::MAX, will trigger resync",
                );
                TimeDelta::MAX
            });
            let drift = wall_elapsed - monotonic_elapsed;

            if drift.abs() > threshold {
                let expected = self.next_tick;
                self.next_tick = align_to_epoch(wall_now, self.interval) + self.interval;
                self.last_wall = wall_now;
                self.last_monotonic = monotonic_now;
                let drift_secs = drift.num_nanoseconds().unwrap_or(0) as f64 / 1e9;
                tracing::warn!(
                    "wall clock jumped (drift={drift_secs:+.3}s); re-syncing, tick fires immediately",
                );
                return TickInfo {
                    expected_tick_time: expected,
                    resynced: true,
                };
            }

            let to_next = self.next_tick - wall_now;
            if to_next <= TimeDelta::zero() {
                let expected = self.next_tick;
                self.next_tick = expected + self.interval;
                self.last_wall = wall_now;
                self.last_monotonic = monotonic_now;
                return TickInfo {
                    expected_tick_time: expected,
                    resynced: false,
                };
            }

            // `to_next` is positive (the `<= zero` branch returned) and
            // bounded by ~2× `interval` — a sub-threshold backward drift
            // can stretch it past one interval, but a larger drift would
            // have hit the resync branch. Since `try_new` validated that
            // `interval` fits in `Duration`, the conversion shouldn't
            // fail; falling back to one `interval` (also known to fit)
            // keeps us out of a busy-loop and lets the next iteration
            // re-evaluate.
            let sleep_for = to_next.to_std().unwrap_or_else(|_| {
                tracing::warn!(
                    "to_next ({to_next:?}) does not fit in std::time::Duration; sleeping for one interval and retrying",
                );
                self.interval
                    .to_std()
                    .unwrap_or(std::time::Duration::from_secs(1))
            });
            tokio::time::sleep(sleep_for).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------- align_to_epoch ----------

    #[test]
    fn test_align_to_epoch_rounds_down() {
        let ts = DateTime::from_timestamp(1_000_005, 0).unwrap();
        let aligned = align_to_epoch(ts, TimeDelta::try_seconds(10).unwrap());
        assert_eq!(aligned.timestamp(), 1_000_000);
    }

    #[test]
    fn test_align_to_epoch_already_aligned() {
        let ts = DateTime::from_timestamp(1_000_000, 0).unwrap();
        let aligned = align_to_epoch(ts, TimeDelta::try_seconds(10).unwrap());
        assert_eq!(aligned, ts);
    }

    #[test]
    fn test_align_to_epoch_sub_second() {
        let ts = DateTime::from_timestamp_millis(1_000_000_750).unwrap();
        let aligned = align_to_epoch(ts, TimeDelta::try_milliseconds(200).unwrap());
        assert_eq!(aligned.timestamp_millis(), 1_000_000_600);
    }

    #[test]
    fn test_align_to_epoch_rounds_down_for_pre_epoch() {
        // Both cases exercise the negative side where truncating division
        // (`/`) rounds toward zero and would give the wrong answer:
        //   -5 at 10s interval: `/` → 0, `div_euclid` → -10 (correct).
        //   -3 at 5s  interval: `/` → 0, `div_euclid` → -5  (correct).
        let ts = DateTime::from_timestamp(-5, 0).unwrap();
        let aligned = align_to_epoch(ts, TimeDelta::try_seconds(10).unwrap());
        assert_eq!(aligned.timestamp(), -10);

        let ts = DateTime::from_timestamp(-3, 0).unwrap();
        let aligned = align_to_epoch(ts, TimeDelta::try_seconds(5).unwrap());
        assert_eq!(aligned.timestamp(), -5);
    }

    #[test]
    fn test_align_to_epoch_unrepresentable_is_identity() {
        // ~500 years past the epoch — beyond the ~292-year i64-nanosecond
        // range, so `num_nanoseconds()` returns None and we fall back to
        // the input timestamp unchanged.
        let ts = DateTime::from_timestamp(500 * 365 * 86400, 0).unwrap();
        assert_eq!(align_to_epoch(ts, TimeDelta::try_seconds(1).unwrap()), ts);
    }

    #[test]
    fn test_align_to_epoch_zero_or_negative_interval_is_identity() {
        let ts = DateTime::from_timestamp_millis(1_000_000_750).unwrap();
        assert_eq!(align_to_epoch(ts, TimeDelta::zero()), ts);
        assert_eq!(align_to_epoch(ts, -TimeDelta::try_seconds(1).unwrap()), ts);
    }

    // ---------- timer ----------

    #[tokio::test(start_paused = true)]
    async fn test_try_new_rejects_non_positive_interval() {
        for bad in [TimeDelta::zero(), -TimeDelta::try_milliseconds(1).unwrap()] {
            let clock = crate::client::test_utils::TokioSyncedClock::new();
            let err = WallClockTimer::try_new(bad, clock)
                .err()
                .unwrap_or_else(|| panic!("expected error for interval {bad:?}"));
            assert_eq!(err.kind(), crate::ErrorKind::InvalidConfig);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_timer_ticks_at_interval() {
        let clock = crate::client::test_utils::TokioSyncedClock::new();
        let interval = TimeDelta::try_milliseconds(20).unwrap();
        let mut timer = WallClockTimer::try_new(interval, clock).unwrap();
        let t1 = timer.tick().await.expected_tick_time;
        let t2 = timer.tick().await.expected_tick_time;
        let t3 = timer.tick().await.expected_tick_time;
        assert_eq!(t2 - t1, interval);
        assert_eq!(t3 - t2, interval);
    }

    #[tokio::test(start_paused = true)]
    async fn test_timer_detects_forward_jump() {
        let clock = crate::client::test_utils::TokioSyncedClock::new();
        let interval = TimeDelta::try_milliseconds(200).unwrap();
        let mut timer = WallClockTimer::try_new(interval, clock.clone()).unwrap();

        let first = timer.tick().await;
        assert!(!first.resynced);

        clock.inject_wall_jump(TimeDelta::try_seconds(30).unwrap());

        let after_jump = timer.tick().await;
        assert!(after_jump.resynced, "expected resync on forward jump");
    }

    #[tokio::test(start_paused = true)]
    async fn test_timer_detects_backward_jump() {
        let clock = crate::client::test_utils::TokioSyncedClock::new();
        let interval = TimeDelta::try_milliseconds(200).unwrap();
        let mut timer = WallClockTimer::try_new(interval, clock.clone()).unwrap();

        let _ = timer.tick().await;

        clock.inject_wall_jump(-TimeDelta::try_seconds(30).unwrap());

        let after_jump = timer.tick().await;
        assert!(after_jump.resynced, "expected resync on backward jump");
    }

    #[tokio::test(start_paused = true)]
    async fn test_timer_first_tick_is_aligned_to_epoch() {
        // Anchor 750 ms past a whole-second boundary; with a 1 s interval the
        // first scheduled tick must land on the next whole second, not
        // 750 ms past one.
        let anchor = DateTime::from_timestamp_millis(1_000_000_750).unwrap();
        let clock = crate::client::test_utils::TokioSyncedClock::with_wall_anchor(anchor);
        let timer = WallClockTimer::try_new(TimeDelta::try_seconds(1).unwrap(), clock).unwrap();
        assert_eq!(timer.next_tick_time().timestamp_millis(), 1_000_001_000);
    }

    #[tokio::test(start_paused = true)]
    async fn test_timer_subthreshold_forward_drift_does_not_resync() {
        let clock = crate::client::test_utils::TokioSyncedClock::new();
        let interval = TimeDelta::try_seconds(1).unwrap();
        let mut timer = WallClockTimer::try_new(interval, clock.clone()).unwrap();
        let _ = timer.tick().await;

        // 500 ms < 1× interval threshold.
        clock.inject_wall_jump(TimeDelta::try_milliseconds(500).unwrap());
        let info = timer.tick().await;
        assert!(!info.resynced);
    }

    #[tokio::test(start_paused = true)]
    async fn test_timer_subthreshold_backward_drift_does_not_resync() {
        let clock = crate::client::test_utils::TokioSyncedClock::new();
        let interval = TimeDelta::try_seconds(1).unwrap();
        let mut timer = WallClockTimer::try_new(interval, clock.clone()).unwrap();
        let _ = timer.tick().await;

        clock.inject_wall_jump(-TimeDelta::try_milliseconds(500).unwrap());
        let info = timer.tick().await;
        assert!(!info.resynced);
    }

    // The threshold uses strict `>`: a jump of *exactly* one interval
    // is treated as scheduling jitter and must NOT resync. These tests
    // pin the boundary so a future change to `>=` would fail loudly.
    #[tokio::test(start_paused = true)]
    async fn test_timer_at_exact_interval_forward_drift_does_not_resync() {
        let clock = crate::client::test_utils::TokioSyncedClock::new();
        let interval = TimeDelta::try_seconds(1).unwrap();
        let mut timer = WallClockTimer::try_new(interval, clock.clone()).unwrap();
        let _ = timer.tick().await;

        clock.inject_wall_jump(interval);
        let info = timer.tick().await;
        assert!(
            !info.resynced,
            "drift of exactly 1× interval should be treated as jitter, not a jump",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_timer_at_exact_interval_backward_drift_does_not_resync() {
        let clock = crate::client::test_utils::TokioSyncedClock::new();
        let interval = TimeDelta::try_seconds(1).unwrap();
        let mut timer = WallClockTimer::try_new(interval, clock.clone()).unwrap();
        let _ = timer.tick().await;

        clock.inject_wall_jump(-interval);
        let info = timer.tick().await;
        assert!(
            !info.resynced,
            "backward drift of exactly 1× interval should be treated as jitter, not a jump",
        );
    }

    // The threshold is `1 × interval`: a jump just above the interval
    // should resync.
    #[tokio::test(start_paused = true)]
    async fn test_timer_detects_forward_jump_just_over_interval() {
        let clock = crate::client::test_utils::TokioSyncedClock::new();
        let interval = TimeDelta::try_seconds(1).unwrap();
        let mut timer = WallClockTimer::try_new(interval, clock.clone()).unwrap();
        let _ = timer.tick().await;

        clock.inject_wall_jump(TimeDelta::try_milliseconds(1500).unwrap());
        let info = timer.tick().await;
        assert!(
            info.resynced,
            "expected resync on forward jump > 1× interval"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_timer_detects_backward_jump_just_over_interval() {
        let clock = crate::client::test_utils::TokioSyncedClock::new();
        let interval = TimeDelta::try_seconds(1).unwrap();
        let mut timer = WallClockTimer::try_new(interval, clock.clone()).unwrap();
        let _ = timer.tick().await;

        clock.inject_wall_jump(-TimeDelta::try_milliseconds(1500).unwrap());
        let info = timer.tick().await;
        assert!(
            info.resynced,
            "expected resync on backward jump > 1× interval"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_timer_resumes_normal_cadence_after_forward_jump() {
        let clock = crate::client::test_utils::TokioSyncedClock::new();
        let interval = TimeDelta::try_milliseconds(200).unwrap();
        let mut timer = WallClockTimer::try_new(interval, clock.clone()).unwrap();
        let _ = timer.tick().await;

        clock.inject_wall_jump(TimeDelta::try_seconds(30).unwrap());
        assert!(timer.tick().await.resynced);

        let a = timer.tick().await;
        let b = timer.tick().await;
        assert!(!a.resynced && !b.resynced);
        assert_eq!(b.expected_tick_time - a.expected_tick_time, interval);
    }

    #[tokio::test(start_paused = true)]
    async fn test_timer_resumes_normal_cadence_after_backward_jump() {
        let clock = crate::client::test_utils::TokioSyncedClock::new();
        let interval = TimeDelta::try_milliseconds(200).unwrap();
        let mut timer = WallClockTimer::try_new(interval, clock.clone()).unwrap();
        let _ = timer.tick().await;

        clock.inject_wall_jump(-TimeDelta::try_seconds(30).unwrap());
        assert!(timer.tick().await.resynced);

        let a = timer.tick().await;
        let b = timer.tick().await;
        assert!(!a.resynced && !b.resynced);
        assert_eq!(b.expected_tick_time - a.expected_tick_time, interval);
    }

    #[tokio::test(start_paused = true)]
    async fn test_timer_resync_expected_tick_time_is_prejump_schedule() {
        // On a resync tick, `TickInfo::expected_tick_time` holds the
        // scheduled pre-jump tick time (not the realigned post-jump time),
        // so callers reading just this field see the tick that *should*
        // have fired.
        let clock = crate::client::test_utils::TokioSyncedClock::new();
        let interval = TimeDelta::try_seconds(1).unwrap();
        let mut timer = WallClockTimer::try_new(interval, clock.clone()).unwrap();
        let _ = timer.tick().await;

        let scheduled_next = timer.next_tick_time();
        clock.inject_wall_jump(TimeDelta::try_seconds(30).unwrap());
        let info = timer.tick().await;
        assert!(info.resynced);
        assert_eq!(info.expected_tick_time, scheduled_next);
    }

    // A caller that takes longer than one interval to re-enter `tick()`
    // (e.g. event-loop lateness, slow downstream work) advances both the
    // wall and monotonic clocks by the same amount. The drift detector
    // must not mistake this for a wall-clock jump.
    #[tokio::test(start_paused = true)]
    async fn test_timer_does_not_resync_on_late_caller() {
        let clock = crate::client::test_utils::TokioSyncedClock::new();
        let interval = TimeDelta::try_milliseconds(200).unwrap();
        let mut timer = WallClockTimer::try_new(interval, clock).unwrap();
        let _ = timer.tick().await;

        // Burn well over one interval of monotonic time without injecting
        // any wall jump — both clocks advance together.
        tokio::time::sleep(std::time::Duration::from_millis(700)).await;

        let info = timer.tick().await;
        assert!(
            !info.resynced,
            "late caller (no wall jump) must not look like a wall-clock jump",
        );
    }

    // Ten consecutive sub-threshold drifts, each +750 ms on a 1 s interval,
    // should all be absorbed without a single resync. The cumulative shift
    // is 7.5 s, but no individual observation exceeds the threshold.
    #[tokio::test(start_paused = true)]
    async fn test_timer_absorbs_many_subthreshold_drifts() {
        let clock = crate::client::test_utils::TokioSyncedClock::new();
        let interval = TimeDelta::try_seconds(1).unwrap();
        let mut timer = WallClockTimer::try_new(interval, clock.clone()).unwrap();
        let _ = timer.tick().await;

        for _ in 0..10 {
            clock.inject_wall_jump(TimeDelta::try_milliseconds(750).unwrap());
            let info = timer.tick().await;
            assert!(!info.resynced, "sub-threshold drift should not resync");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_timer_next_tick_time_realigns_after_jump() {
        let clock = crate::client::test_utils::TokioSyncedClock::new();
        let interval = TimeDelta::try_seconds(1).unwrap();
        let mut timer = WallClockTimer::try_new(interval, clock.clone()).unwrap();
        let _ = timer.tick().await;

        let before_next = timer.next_tick_time();
        clock.inject_wall_jump(TimeDelta::try_seconds(30).unwrap());
        let _ = timer.tick().await;
        let after_next = timer.next_tick_time();

        // The realigned next_tick_time should be ~30 s past where it was
        // before (give or take the alignment offset within one interval).
        let shift = (after_next - before_next).num_seconds();
        assert!(
            (29..=31).contains(&shift),
            "expected ~30s shift, got {shift}s"
        );
    }
}
