# Frequenz Microgrid Release Notes

## New Features

- New `PvPool` type (accessible via `Microgrid::pv_pool`) exposing:
  - `power()` — a `Formula<Power>` for the pool's aggregated power.
  - `power_bounds()` — a `broadcast::Receiver<Vec<Bounds<Power>>>` tracking the pool's power bounds.
  - `telemetry_snapshots()` — a `broadcast::Receiver<PvPoolSnapshot>` partitioning the pool's inverters into healthy and unhealthy sets.

- `BatteryPool::telemetry_snapshots()` exposes the same per-component health partition (`BatteryPoolSnapshot`).

- The pool telemetry trackers and snapshot types (`BatteryPoolSnapshot`, `PvPoolSnapshot`, `InverterBatteryGroup`, `InverterBatteryGroupStatus`) are now public and re-exported from the crate root.
