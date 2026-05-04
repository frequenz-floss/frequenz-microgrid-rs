# Frequenz Microgrid Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The `ChronoError` variant has been removed from the `ErrorKind` enum. This is a breaking change for code that pattern-matches on `ErrorKind`. The variant had no remaining constructors; the time-delta arithmetic that previously surfaced it is now performed inside the new `WallClockTimer`, and resampling-interval misconfiguration is reported as `InvalidConfig`.

- `LogicalMeterConfig` instances can't be created directly anymore, and need to be created using the `LogicalMeterConfig::new` method.  This helps avoid future breaking changes, as we add more config parameters.

- Formula streaming methods in the `LogicalMeterHandle` no longer take metric as a function parameter, but expect a generic argument.  For example:

  *Old syntax:*
   ```rust
   let formula_grid = logical_meter.grid(metric::AcPowerActive)?;
   let formula_pv = logical_meter.pv(None, metric::AcVoltage)?;
   ```

  *New syntax:*
   ```rust
   let formula_grid = logical_meter.grid::<metric::AcPowerActive>()?;
   let formula_pv = logical_meter.pv::<metric::AcVoltage>(None)?;
   ```

## New Features

- It is now possible to change the default resampling function, and to override the resampling function for specific metrics.

- The resampler's `max_age_in_intervals` has also become configurable, through `LogicalMeterConfig`.

- The `Quantity` trait now exposes numeric operations (`abs`, `floor`, `ceil`, `round`, `trunc`, `fract`, `is_nan`, `is_infinite`, `min`, `max`) as trait methods, so they can be used in generic code over any `Quantity` (including `f32` and all quantity types).

- The `Quantity` trait now provides associated `MIN` and `MAX` constants.

- New `BatteryPool` type (accessible via `Microgrid::battery_pool`) exposing:
  - `power()` — a `Formula<Power>` for the pool's aggregated power.
  - `power_bounds()` — a `broadcast::Receiver<Vec<Bounds<Power>>>` tracking the pool's power bounds.

- The logical meter now survives NTP-style wall-clock jumps. A new internal `WallClockTimer` schedules resampler ticks against the wall clock while sleeping on tokio's monotonic clock; when the two diverge by more than one interval in either direction, the timer realigns and the actor rebuilds its inner resamplers against the new clock frame. Subscribers see one `None` sample at the resync tick (preserving the every-interval cadence) and real values resume on the next tick. Note: a backward jump produces a sample timestamped in the past relative to the previous one — consumers that assume monotonically increasing `Sample` timestamps must handle this.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
