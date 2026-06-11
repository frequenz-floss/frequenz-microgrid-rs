# Frequenz Microgrid Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- `BatteryPoolTelemetryTracker` and `PvPoolTelemetryTracker` are no longer public; they were an implementation detail. Use `BatteryPool::telemetry_snapshots()` / `PvPool::telemetry_snapshots()` to consume their snapshots.

- `PvPoolSnapshot` now exposes a single `inverters: ComponentHealthPartition` instead of the separate `healthy_inverters` / `unhealthy_inverters` maps:

  - `snapshot.healthy_inverters` → `snapshot.inverters.healthy`
  - `snapshot.unhealthy_inverters` → `snapshot.inverters.unhealthy`

- `InverterBatteryGroupStatus` (reached via `BatteryPoolSnapshot::groups()`) now groups its telemetry into `inverters: ComponentHealthPartition` and `batteries: ComponentHealthPartition`:

  - `status.healthy_inverters` → `status.inverters.healthy`
  - `status.unhealthy_inverters` → `status.inverters.unhealthy`
  - `status.healthy_batteries` → `status.batteries.healthy`
  - `status.unhealthy_batteries` → `status.batteries.unhealthy`

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

- The pool, group, and component telemetry trackers no longer leak their tasks (while logging at error level every tick) once their consumers are gone; normal shutdown is now logged at debug.

- The client now evicts ended per-component telemetry streams from its cache, so a pool recreated on the same client receives telemetry again instead of silently getting none.
