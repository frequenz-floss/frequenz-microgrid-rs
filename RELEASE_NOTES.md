# Frequenz Microgrid Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- `BatteryPoolTelemetryTracker` and `PvPoolTelemetryTracker` are no longer public; they were an implementation detail. Use `BatteryPool::telemetry_snapshots()` / `PvPool::telemetry_snapshots()` to consume their snapshots.

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

- The pool, group, and component telemetry trackers no longer leak their tasks (while logging at error level every tick) once their consumers are gone; normal shutdown is now logged at debug.

- The client now evicts ended per-component telemetry streams from its cache, so a pool recreated on the same client receives telemetry again instead of silently getting none.
