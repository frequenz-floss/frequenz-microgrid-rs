# Frequenz Microgrid Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

- `LogicalMeterHandle::steam_boiler::<M>()` streams a metric for a set of steam boilers.
- `Microgrid::steam_boiler_pool()` returns a `SteamBoilerPool` with the pool's active power, aggregated active-power bounds and health-partitioned telemetry snapshots.
- `test-utils`: `MockComponent::steam_boiler()` builds a steam boiler, and `MockComponent::add_power_bounds()` attaches bounds to a component's streamed active-power samples.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
